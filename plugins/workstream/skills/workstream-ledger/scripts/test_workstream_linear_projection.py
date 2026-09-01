#!/usr/bin/env python3
from __future__ import annotations

import base64
from contextlib import nullcontext
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import re
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
import uuid
import zlib

from workstream_checkpoint import build_checkpoint
from workstream_choices import record_choice
from workstream_delta import Delta
from workstream_evidence import evidence_errors
from workstream_linear_checkpoints import (
    encode_checkpoint_comment, LinearCheckpointError,
    reduce_checkpoint_comments,
)
from workstream_linear import bootstrap_linear_route, LinearGraphQLTransport
from workstream_linear_events import (
    encode_event_comment, LinearCommentEventAdapter, reduce_event_comments,
)
from workstream_generation import (
    _digest as generation_digest, build_retirement_proof, GenerationTransport,
    generation_quarantine_metadata, reduce_generation_checkpoint_comments,
    prepare_generation_operator_contract,
)
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, LinearProjectionAdapter,
    LinearProjectionError, projection_slot_id, reduce_projection_comments,
    select_plan_generation, TOMBSTONE,
)
from workstream_resume import (
    add_child_material_history, add_live_child_material_history,
    add_material_history, compact_context as resume_compact_context, ResumeError,
)
from workstream_relation_readback import RelationReadbackError
import workstream_projection
import workstream_generation
import workstream_linear_projection as projection_module
import workstream_resume as resume_module
import workstream_shipyard_profile as shipyard_profile
from workstream_projection import (
    _fence_predecessor_projection_history,
    load_material_history_for_projection_reconcile, projection_review_contract,
    prepare_terminal_child_evidence_seeds, prepare_terminal_child_repairs,
    ProjectionPreviewAdapter, projection_preview_sha256,
    require_matching_projection_preview,
    reconcile_required_projection,
    stable_live_readback,
    terminal_child_evidence_seed_predecessor_contract,
)
from workstream_successor import choose_disposition
from workstream_scope import repository_key
from workstream_projection_history import (
    closure_bound_historical_evidence, ProjectionHistoryError,
)
from workstream_closure import review as closure_review
from workstream_child_closure import (
    canonical_digest, ChildClosureError, evidence_receipts_sha256,
    terminal_child_readback,
)
from workstream_child_dependencies import dependency_root_readback_sha256


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


def compact_context(*args, **kwargs):
    """Exercise pre-graph projection fixtures through the legacy constructor."""
    kwargs.setdefault("require_dependency_graph", False)
    return resume_compact_context(*args, **kwargs)


def mock_child_uuid(identifier):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"linear.test/{identifier}"))


MOCK_CHILD_IDS = {}


def live_graph_with_empty_child_comments(graph):
    """Shape a mocked transport response like include_child_comments=True."""
    result = deepcopy(graph)
    result["root"].setdefault("id", ROOT_UUID)
    result["root"].setdefault("team", {
        "id": AUTHORITY["team_id"],
        "organization": {"id": AUTHORITY["workspace_id"]},
    })
    result["root"].setdefault("project", {"id": AUTHORITY["project_id"]})
    for index, child in enumerate(result.get("children", [])):
        child.setdefault("id", mock_child_uuid(child["identifier"]))
        MOCK_CHILD_IDS[str(child["identifier"]).upper()] = child["id"]
        child.setdefault("description", f"Plan revision: {result['root']['plan_revision']}")
        child.setdefault("parent", {
            "id": ROOT_UUID, "identifier": "GEN-37",
        })
        child.setdefault("team", {
            "id": AUTHORITY["team_id"],
            "organization": {"id": AUTHORITY["workspace_id"]},
        })
        child.setdefault("project", {"id": AUTHORITY["project_id"]})
    result["child_comments"] = {
        str(child.get("identifier", "")).upper(): []
        for child in result.get("children", [])
        if str(child.get("status_type") or child.get("status") or "").lower()
        not in {"done", "completed", "cancelled", "canceled", "superseded"}
    }
    return result


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


def fixed_terminal_fence(value):
    return lambda child_ids: {child_id: value for child_id in child_ids}


def acknowledged_checkpoint(checkpoint_id, *, head=HEAD):
    return {
        "checkpoint_event_id": checkpoint_id,
        "worktree": {"state": "safe", "head": head},
        "acknowledgement": {
            "state": "remote_acknowledged",
            "remote_id": f"comment-{checkpoint_id}",
            "applied_revision": 0,
        },
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
        if (
            "query WorkstreamChildRelations" in query
            or "query WorkstreamChildInverseRelations" in query
        ):
            identifier = variables["issueId"].upper()
            field = (
                "inverseRelations"
                if "query WorkstreamChildInverseRelations" in query
                else "relations"
            )
            return {"issue": {
                "id": MOCK_CHILD_IDS.get(identifier, mock_child_uuid(identifier)),
                "identifier": identifier,
                "parent": {"id": ROOT_UUID, "identifier": "GEN-37"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                field: {"nodes": [], "pageInfo": {
                    "hasNextPage": False, "endCursor": None,
                }},
            }}
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
                       "createdAt": f"2026-08-20T00:00:{len(self.comments):02d}Z",
                       "updatedAt": f"2026-08-20T00:00:{len(self.comments):02d}Z"}
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
    def setUp(self):
        # CLI integration cases below exercise the apply/readback path. Keep
        # their reviewed preview receipt deterministic while the dedicated
        # preview tests exercise the real digest function.
        patcher = mock.patch.object(
            workstream_projection, "projection_preview_sha256",
            return_value="a" * 64,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_projection_cli_refuses_implicit_apply_before_any_live_access(self):
        argv = [
            "workstream_projection.py", "GEN-37", "manifest.json",
            "--remote-head", HEAD, "--plan-source", "PLAN.md",
        ]
        with mock.patch.object(workstream_projection.sys, "argv", argv), \
             mock.patch.object(
                 workstream_projection, "load_linear_api_key",
             ) as load_key, \
             self.assertRaises(SystemExit) as refused:
            workstream_projection.main()
        self.assertEqual(refused.exception.code, 2)
        load_key.assert_not_called()

    def test_preview_adapter_is_deterministic_and_cannot_write_linear(self):
        client = FakeProjectionClient()
        live = self.authorization_adapter(client)
        preview = ProjectionPreviewAdapter(live, client.comments)
        event = build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"identity": "https://example.test/plan", "sha256": PLAN},
            plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-31T23:00:00Z", authority=AUTHORITY,
        )
        first = preview.append(event)
        self.assertEqual(client.comments, [])
        self.assertEqual(client.calls, [])
        self.assertEqual(preview.state().revision, 1)
        self.assertEqual(first["remote_id"], projection_slot_id(
            "GEN-37", PLAN, 0, AUTHORITY,
        ))
        surface = {"writes": preview.receipts, "apply": False}
        self.assertEqual(
            projection_preview_sha256(surface),
            projection_preview_sha256(deepcopy(surface)),
        )

    def test_safe_projection_apply_requires_exact_preview_digest_and_timestamp(self):
        require_matching_projection_preview(
            created_at="2026-08-31T23:00:00Z",
            expected_sha256="a" * 64, observed_sha256="a" * 64,
        )
        for created_at, expected in ((None, "a" * 64), ("now", "b" * 64)):
            with self.assertRaisesRegex(
                LinearProjectionError,
                "projection_apply_requires_matching_reviewed_preview",
            ):
                require_matching_projection_preview(
                    created_at=created_at, expected_sha256=expected,
                    observed_sha256="a" * 64,
                )

    @staticmethod
    def authorization_adapter(client):
        return LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )

    @staticmethod
    def reserve_child(adapter, **overrides):
        values = {
            "source": {"identity": "plan:legacy", "sha256": PLAN},
            "reviewed_candidate_key": "candidate-a",
            "child_issue_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "expected_material_revision": 0,
            "expected_projection_revision": 0,
            "native_initialization": {
                "state_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "assignee_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            },
            "generation_authority": {
                "plan_revision": PLAN,
                "description_plan_revision": PLAN,
                "transition_tip_event_id": None,
                "activation_epoch": None,
                "authority_origin": "legacy_description",
                "workstream_id": "GEN-37",
                "authority": AUTHORITY,
                "source": {"identity": "plan:legacy", "sha256": PLAN},
            },
            "native_validation_sha256": "0" * 64,
            "child_content": {
                "schema_version": 1,
                "title": "Candidate A",
                "description_sha256": "1" * 64,
            },
        }
        values.update(overrides)
        return adapter.reserve_child_extension(**values)

    def test_child_extension_authorization_is_durable_and_replay_is_noop(self):
        client = FakeProjectionClient()
        adapter = self.authorization_adapter(client)

        first = self.reserve_child(adapter)
        replay = self.reserve_child(
            adapter, expected_projection_revision=1,
        )

        self.assertEqual(first["event"], replay["event"])
        self.assertEqual(first["remote_id"], replay["remote_id"])
        self.assertEqual(len(client.comments), 1)
        self.assertEqual(first["event"]["kind"], "child_extension_authorization")
        self.assertEqual(
            first["event"]["value"]["initial_state"],
            "planned_pending_projection",
        )
        self.assertEqual(first["event"]["value"]["native_initialization"], {
            "state_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "assignee_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        })
        self.assertEqual(first["event"]["value"]["child_content"], {
            "schema_version": 1,
            "title": "Candidate A",
            "description_sha256": "1" * 64,
        })
        self.assertEqual(first["disposition"], "created")
        self.assertEqual(replay["disposition"], "existing")

    def test_child_extension_authorization_binds_exact_content_and_schema(self):
        client = FakeProjectionClient()
        adapter = self.authorization_adapter(client)
        self.reserve_child(adapter)

        for child_content in (
            {
                "schema_version": 1, "title": "Changed title",
                "description_sha256": "1" * 64,
            },
            {
                "schema_version": 1, "title": "Candidate A",
                "description_sha256": "2" * 64,
            },
            {
                "schema_version": 2, "title": "Candidate A",
                "description_sha256": "1" * 64,
            },
        ):
            with self.subTest(child_content=child_content):
                with self.assertRaisesRegex(
                    LinearProjectionError,
                    "child_extension_authorization_superseded_or_conflicting|"
                    "invalid_child_extension_content",
                ):
                    self.reserve_child(
                        adapter, expected_projection_revision=1,
                        child_content=child_content,
                    )
        self.assertEqual(len(client.comments), 1)

    def test_contentless_current_authorization_only_replays_existing_child(self):
        client = FakeProjectionClient()
        adapter = self.authorization_adapter(client)
        current = self.reserve_child(adapter)["event"]
        contentless_value = {
            key: value for key, value in current["value"].items()
            if key != "child_content"
        }
        contentless = build_projection_event(
            workstream_id="GEN-37", kind="child_extension_authorization",
            key=current["key"], value=contentless_value,
            plan_revision=PLAN, expected_revision=0,
            created_at="1970-01-01T00:00:00Z", authority=AUTHORITY,
        )
        client.comments[:] = [{
            "id": projection_slot_id("GEN-37", PLAN, 0, AUTHORITY),
            "body": encode_projection_comment(contentless),
            "createdAt": "1970-01-01T00:00:00Z",
            "updatedAt": "1970-01-01T00:00:00Z",
        }]

        replay = self.reserve_child(
            adapter, expected_projection_revision=1, require_existing=True,
        )
        self.assertEqual(replay["disposition"], "legacy_content_existing")
        self.assertEqual(len(client.comments), 1)

        with self.assertRaisesRegex(
            LinearProjectionError,
            "legacy_content_authorization_requires_existing_child",
        ):
            self.reserve_child(
                adapter, expected_projection_revision=1,
                require_existing=False,
            )
        self.assertEqual(len(client.comments), 1)

    def test_preexisting_child_requires_preexisting_authorization(self):
        client = FakeProjectionClient()
        adapter = self.authorization_adapter(client)

        with self.assertRaisesRegex(
            LinearProjectionError,
            "child_extension_preexisting_child_without_authorization",
        ):
            self.reserve_child(adapter, require_existing=True)

        self.assertEqual(client.comments, [])

    def test_child_extension_boolean_frontiers_refuse_before_remote_access(self):
        class NoRemoteAccessClient(FakeProjectionClient):
            def execute(self, query, variables):
                raise AssertionError("invalid frontier must fail before remote access")

        for overrides, message in (
            ({"expected_material_revision": False},
             "invalid_child_extension_material_frontier"),
            ({"expected_projection_revision": False},
             "invalid_child_extension_projection_frontier"),
        ):
            client = NoRemoteAccessClient()
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(LinearProjectionError, message):
                    self.reserve_child(self.authorization_adapter(client), **overrides)
            self.assertEqual(client.comments, [])

    def test_child_extension_authorization_lost_response_converges(self):
        class LostResponseClient(FakeProjectionClient):
            def __init__(self):
                super().__init__()
                self.lost = False

            def execute(self, query, variables):
                response = super().execute(query, variables)
                if "commentCreate" in query and not self.lost:
                    self.lost = True
                    raise TimeoutError("authorization response lost after commit")
                return response

        client = LostResponseClient()
        receipt = self.reserve_child(self.authorization_adapter(client))

        self.assertEqual(len(client.comments), 1)
        self.assertEqual(receipt["remote_id"], client.comments[0]["id"])

    def test_later_material_event_does_not_revoke_child_authorization(self):
        client = FakeProjectionClient()
        adapter = self.authorization_adapter(client)
        first = self.reserve_child(adapter)
        later = Delta(
            "material-later", "GEN-37", "requirement", "agent",
            {"text": "later"}, 0, "2026-08-20T00:00:01Z",
        )
        client.comments.append({
            "id": "material-later-comment", "body": encode_event_comment(later),
            "createdAt": "2026-08-20T00:00:01Z",
            "updatedAt": "2026-08-20T00:00:01Z",
        })

        replay = self.reserve_child(
            adapter, expected_material_revision=1,
            expected_projection_revision=1,
        )

        self.assertEqual(replay["event"], first["event"])
        self.assertEqual(len(client.comments), 2)

    def test_child_extension_authorization_loses_planted_projection_slot_race(self):
        winner = build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"identity": "plan:legacy", "sha256": PLAN},
            plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-20T00:00:00Z", authority=AUTHORITY,
        )
        client = RacingProjectionClient(winner)

        with self.assertRaisesRegex(
            LinearProjectionError, "projection_slot_lost_reload_required",
        ):
            self.reserve_child(self.authorization_adapter(client))

        state = self.authorization_adapter(client).state()
        self.assertFalse(any(
            event["kind"] == "child_extension_authorization"
            for event in state.events
        ))

    def test_material_event_before_authorization_is_refused_by_remote_order(self):
        class MaterialRaceClient(FakeProjectionClient):
            def __init__(self):
                super().__init__()
                self.injected = False

            def execute(self, query, variables):
                if "commentCreate" in query and not self.injected:
                    self.injected = True
                    event = Delta(
                        "material-race", "GEN-37", "requirement", "agent",
                        {"text": "raced"}, 0, "2026-08-20T00:00:00Z",
                    )
                    self.comments.append({
                        "id": "material-comment",
                        "body": encode_event_comment(event),
                        "createdAt": "2026-08-20T00:00:00Z",
                        "updatedAt": "2026-08-20T00:00:00Z",
                    })
                return super().execute(query, variables)

        client = MaterialRaceClient()
        with self.assertRaisesRegex(
            LinearProjectionError,
            "child_extension_material_preceded_authorization_reload_required",
        ):
            self.reserve_child(self.authorization_adapter(client))

        # The planted race may reserve authority, but callers never receive an
        # executable grant and therefore must not create the child.
        self.assertEqual(len(client.comments), 2)

    def test_superseded_child_extension_authorization_refuses_readback(self):
        client = FakeProjectionClient()
        adapter = self.authorization_adapter(client)
        receipt = self.reserve_child(adapter)
        original = receipt["event"]
        value = deepcopy(original["value"])
        value["expected_projection_revision"] = 1
        replacement = build_projection_event(
            workstream_id="GEN-37", kind="child_extension_authorization",
            key=original["key"], value=value, plan_revision=PLAN,
            expected_revision=1, created_at="1970-01-01T00:00:00Z",
            supersedes_event_id=original["event_id"], authority=AUTHORITY,
        )
        adapter.append(replacement)

        with self.assertRaisesRegex(
            LinearProjectionError,
            "child_extension_authorization_superseded_or_conflicting",
        ):
            adapter.assert_child_extension_authorized(original)

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

    def mixed_head_plan_generation_fixture(
        self, *, current_secondary=False, activation_ready=False,
    ):
        predecessor_plan = "b" * 64
        first_head = "1" * 40
        second_head = "2" * 40
        client = FakeProjectionClient()
        predecessor = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=predecessor_plan, **AUTHORITY,
        )
        identifiers = ["GEN-38", "GEN-39", "GEN-40", "GEN-41", "GEN-85"]
        if current_secondary:
            identifiers.append("GEN-42")
        children = []
        for offset, identifier in enumerate(identifiers, start=1):
            children.append({
                "id": f"{offset:08d}-0000-4000-8000-{offset:012d}",
                "identifier": identifier, "title": f"terminal {identifier}",
                "parent": {"id": ROOT_UUID, "identifier": "GEN-37"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                "assignee": {"id": f"owner-{identifier}"},
                "state_id": f"done-{identifier}", "status": "Done",
                "status_type": "completed",
            })
        predecessor_scope = scope()
        predecessor_scope["repositories"][0]["exact_head"] = first_head
        predecessor_scope["child_ownership"] = {
            identifier: predecessor_scope["primary_repository"]
            for identifier in identifiers
        }

        def append(kind, key, value):
            previous = next((
                event for event in reversed(predecessor.state().events)
                if (event["kind"], event["key"]) == (kind, key)
            ), None)
            return predecessor.append(build_projection_event(
                workstream_id="GEN-37", kind=kind, key=key, value=value,
                plan_revision=predecessor_plan,
                expected_revision=predecessor.state().revision,
                created_at=(
                    f"2026-08-29T20:{predecessor.state().revision:02d}:00Z"
                ),
                supersedes_event_id=(previous or {}).get("event_id"),
                authority=AUTHORITY,
            ))

        append("scope", "root", predecessor_scope)
        append("source", "root", {
            "identity": "https://example.test/previous-plan",
            "sha256": predecessor_plan,
        })
        append("provenance", "previous", {
            "agent": "codex", "machine": "M5", "session_id": "previous",
            "worktree": {"state": "safe", "head": second_head},
        })
        contracts = {}
        for child in children[:4]:
            contract = evidence_contract()
            contract["slice_id"] = f"{child['identifier'].lower()}-terminal"
            contract["owning_child"] = child["identifier"]
            contract["plan_revision"] = predecessor_plan
            contract["exact_head"] = first_head
            for layer in contract["layers"].values():
                for receipt in layer.get("receipts", []):
                    receipt["exact_head"] = first_head
            append("evidence_contract", contract["slice_id"], contract)
            evidence_event = predecessor.state().events[-1]
            readback = terminal_child_readback(child)
            closure = {
                "schema_version": 2, **readback,
                "plan_revision": predecessor_plan,
                "repository_key": contract["repository_key"],
                "exact_head": first_head,
                "evidence_heads": [{
                    "key": evidence_event["key"],
                    "event_id": evidence_event["event_id"],
                    "value_sha256": canonical_digest(evidence_event["value"]),
                }],
                "evidence_receipts_sha256": evidence_receipts_sha256([contract]),
                "child_readback_sha256": canonical_digest(readback),
            }
            append("child_closure", child["identifier"], closure)
            contracts[contract["slice_id"]] = contract
        second_scope = deepcopy(predecessor_scope)
        second_scope["repositories"][0]["exact_head"] = second_head
        if current_secondary:
            shipyard = deepcopy(second_scope["repositories"][0])
            shipyard.update({
                "provider_repository_id": "shipyard-id",
                "slug": "github.com/acme/shipyard",
                "exact_head": "d513461fed3571a18f748aa9dd939d5c431ee957",
                "identity_resolution": {
                    "provider_repository_id": "shipyard-id",
                    "resolved_slug": "github.com/acme/shipyard",
                    "observed_at": "2026-08-29T20:00:00Z",
                    "evidence": [{
                        "kind": "authenticated_provider_readback",
                        "authenticated": True,
                        "provider_repository_id": "shipyard-id",
                        "resolved_slug": "github.com/acme/shipyard",
                    }],
                },
            })
            second_scope["repositories"].append(shipyard)
            second_scope["child_ownership"]["GEN-42"] = (
                "github.com:id:shipyard-id"
            )
        append("scope", "root", second_scope)
        child = next(
            item for item in children if item["identifier"] == "GEN-85"
        )
        contract = evidence_contract()
        contract["slice_id"] = "gen-85-terminal"
        contract["owning_child"] = "GEN-85"
        contract["plan_revision"] = predecessor_plan
        contract["exact_head"] = second_head
        for layer in contract["layers"].values():
            for receipt in layer.get("receipts", []):
                receipt["exact_head"] = second_head
        append("evidence_contract", contract["slice_id"], contract)
        evidence_event = predecessor.state().events[-1]
        readback = terminal_child_readback(child)
        append("child_closure", "GEN-85", {
            "schema_version": 2, **readback,
            "plan_revision": predecessor_plan,
            "repository_key": contract["repository_key"],
            "exact_head": second_head,
            "evidence_heads": [{
                "key": evidence_event["key"],
                "event_id": evidence_event["event_id"],
                "value_sha256": canonical_digest(evidence_event["value"]),
            }],
            "evidence_receipts_sha256": evidence_receipts_sha256([contract]),
            "child_readback_sha256": canonical_digest(readback),
        })
        contracts[contract["slice_id"]] = contract
        if current_secondary:
            child = next(
                item for item in children if item["identifier"] == "GEN-42"
            )
            contract = evidence_contract()
            contract.update({
                "slice_id": "gen-42-terminal",
                "owning_child": "GEN-42",
                "plan_revision": predecessor_plan,
                "repository": "github.com/acme/shipyard",
                "repository_key": "github.com:id:shipyard-id",
                "exact_head": shipyard["exact_head"],
            })
            for layer in contract["layers"].values():
                for receipt in layer.get("receipts", []):
                    receipt.update({
                        "repository_key": "github.com:id:shipyard-id",
                        "exact_head": shipyard["exact_head"],
                    })
            append("evidence_contract", contract["slice_id"], contract)
            evidence_event = predecessor.state().events[-1]
            readback = terminal_child_readback(child)
            append("child_closure", "GEN-42", {
                "schema_version": 2, **readback,
                "plan_revision": predecessor_plan,
                "repository_key": contract["repository_key"],
                "exact_head": shipyard["exact_head"],
                "evidence_heads": [{
                    "key": evidence_event["key"],
                    "event_id": evidence_event["event_id"],
                    "value_sha256": canonical_digest(evidence_event["value"]),
                }],
                "evidence_receipts_sha256": evidence_receipts_sha256([contract]),
                "child_readback_sha256": canonical_digest(readback),
            })
            contracts[contract["slice_id"]] = contract
        if activation_ready:
            append("disposition", "root", {
                "disposition": "attach", "remote_head": second_head,
                "recovered_from_checkpoint": None,
            })
            while predecessor.state().revision < 85:
                revision = predecessor.state().revision
                append("provenance", f"production-padding-{revision}", {
                    "agent": "codex", "machine": "M5",
                    "session_id": f"production-padding-{revision}",
                    "worktree": {"state": "safe", "head": second_head},
                })

        material = Delta(
            "plan-transition", "GEN-37", "requirement", "agent",
            {"text": "Reconcile the new plan generation."}, 0,
            "2026-08-29T21:00:00Z",
        )
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="predecessor", root_revision=1,
            plan_revision=predecessor_plan, before_status="In Progress",
            after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "predecessor", "machine": "M5",
                "worktree": {
                    "state": "safe", "path": "/worktree", "branch": "previous",
                    "head": second_head,
                },
            },
            exact_head=second_head, evidence=[], blocker=None,
            next_action="Reconcile the new plan generation.",
        )
        client.comments.extend([
            {"id": "material-plan-transition", "body": encode_event_comment(material)},
            {"id": "checkpoint-predecessor", "body": encode_checkpoint_comment(checkpoint)},
        ])
        graph = self.graph_snapshot()
        graph["root"]["revision"] = 1
        graph["children"] = children
        current = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        desired_scope = deepcopy(second_scope)
        desired_scope["repositories"][0]["exact_head"] = HEAD
        desired_contracts = {}
        for key, previous_contract in contracts.items():
            desired_contracts[key] = deepcopy(previous_contract)
            desired_contracts[key]["plan_revision"] = PLAN
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        projection = [
            {"kind": "scope", "key": "root", "value": desired_scope},
            {"kind": "source", "key": "root", "value": source},
            {"kind": "provenance", "key": "current", "value": {
                "agent": "codex", "machine": "M5", "session_id": "current",
                "worktree": {"state": "safe", "head": HEAD},
            }},
            *[
                {"kind": "evidence_contract", "key": key, "value": value}
                for key, value in sorted(desired_contracts.items())
            ],
        ]
        seeds = [
            {
                "child_identifier": child["identifier"],
                "child_issue_id": child["id"],
                "expected_child_readback_sha256": canonical_digest(
                    terminal_child_readback(child)
                ),
                "expected_assignee_id": child["assignee"]["id"],
                "evidence_keys": [f"{child['identifier'].lower()}-terminal"],
            }
            for child in sorted(children, key=lambda item: item["identifier"])
        ]
        binding, _authorities = terminal_child_evidence_seed_predecessor_contract(
            graph, current.state(), client.comments, workstream_id="GEN-37",
            predecessor_plan_revision=predecessor_plan,
            desired_scope=desired_scope, seeds=seeds,
            desired_contracts=desired_contracts,
        )
        manifest = {
            **reviewed_manifest(current, projection),
            "terminal_child_evidence_seeds": seeds,
            "terminal_child_evidence_seed_predecessor": binding,
        }
        return client, current, source, graph, children, manifest, binding

    def test_generation_prepare_manifest_matches_projection_seed_preview_at_revision_11(self):
        """A transport graph must have one producer/consumer frontier."""
        predecessor_plan = "b" * 64
        child = {
            "id": "70000000-0000-4000-8000-000000000070",
            "identifier": "GEN-70", "title": "terminal GEN-70",
            "parent": {"id": ROOT_UUID, "identifier": "GEN-37"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project"}, "assignee": {"id": "owner-70"},
            "state_id": "done-70", "status": "Done",
            "status_type": "completed",
        }
        client = FakeProjectionClient()
        predecessor = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=predecessor_plan, **AUTHORITY,
        )
        predecessor_scope = scope()
        pulp_repository = deepcopy(predecessor_scope["repositories"][0])
        pulp_repository.update({
            "slug": "github.com/generous-corp/pulp",
            "provider_repository_id": "R_pulp",
            "exact_head": "9" * 40,
        })
        pulp_repository["identity_resolution"].update({
            "provider_repository_id": "R_pulp",
            "resolved_slug": "github.com/generous-corp/pulp",
        })
        pulp_repository["identity_resolution"]["evidence"][0].update({
            "provider_repository_id": "R_pulp",
            "resolved_slug": "github.com/generous-corp/pulp",
        })
        predecessor_scope["repositories"].append(pulp_repository)
        predecessor_scope["child_ownership"] = {
            "GEN-70": "github.com:id:R_pulp",
        }

        def append(kind, key, value):
            return predecessor.append(build_projection_event(
                workstream_id="GEN-37", kind=kind, key=key, value=value,
                plan_revision=predecessor_plan,
                expected_revision=predecessor.state().revision,
                created_at=f"2026-09-01T07:{predecessor.state().revision:02d}:00Z",
                authority=AUTHORITY,
            ))

        append("scope", "root", predecessor_scope)
        append("source", "root", {
            "identity": "https://example.test/previous-plan",
            "sha256": predecessor_plan,
        })
        append("provenance", "previous", {
            "agent": "codex", "machine": "M3", "session_id": "previous",
            "worktree": {"state": "safe", "head": HEAD},
        })
        contract = evidence_contract()
        contract.update({
            "slice_id": "gen-70-terminal-receipts-v1",
            "owning_child": "GEN-70", "plan_revision": predecessor_plan,
            "repository": pulp_repository["slug"],
            "repository_key": "github.com:id:R_pulp",
            "exact_head": pulp_repository["exact_head"],
        })
        for layer in contract["layers"].values():
            for receipt in layer.get("receipts", []):
                receipt.update({
                    "repository_key": "github.com:id:R_pulp",
                    "exact_head": pulp_repository["exact_head"],
                })
        append("evidence_contract", contract["slice_id"], contract)
        evidence_event = predecessor.state().events[-1]
        readback = terminal_child_readback(child)
        append("child_closure", "GEN-70", {
            "schema_version": 2, **readback,
            "plan_revision": predecessor_plan,
            "repository_key": contract["repository_key"],
            "exact_head": contract["exact_head"],
            "evidence_heads": [{
                "key": evidence_event["key"],
                "event_id": evidence_event["event_id"],
                "value_sha256": canonical_digest(evidence_event["value"]),
            }],
            "evidence_receipts_sha256": evidence_receipts_sha256([contract]),
            "child_readback_sha256": canonical_digest(readback),
        })
        for index in range(6):
            append("provenance", f"revision-11-{index}", {
                "agent": "codex", "machine": "M3",
                "session_id": f"revision-11-{index}",
                "worktree": {"state": "safe", "head": HEAD},
            })
        self.assertEqual(predecessor.state().revision, 11)

        material = Delta(
            "generation-boundary", "GEN-37", "requirement", "agent",
            {"text": "Carry terminal children into the next plan."}, 0,
            "2026-09-01T07:20:00Z",
        )
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="generation-boundary",
            root_revision=1, plan_revision=predecessor_plan,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "previous", "machine": "M3",
                "worktree": {
                    "state": "safe", "path": "/worktree",
                    "branch": "previous", "head": HEAD,
                },
            },
            exact_head=HEAD, evidence=[], blocker=None,
            next_action="Carry terminal children into the next plan.",
        )
        client.comments.extend([
            {"id": "material-generation-boundary",
             "body": encode_event_comment(material)},
            {"id": "checkpoint-generation-boundary",
             "body": encode_checkpoint_comment(checkpoint)},
        ])
        raw_graph = {
            "root": {
                "id": ROOT_UUID, "identifier": "GEN-37",
                "url": "https://linear.test/GEN-37",
                "plan_revision": predecessor_plan, "revision": 1,
                "status": "In Progress", "status_type": "started",
                "next_action": "Carry terminal children into the next plan.",
            },
            "children": [child], "child_comments": {},
        }
        with mock.patch(
            "workstream_projection.projection_disposition_value",
            side_effect=AssertionError(
                "seed preview must not derive an activation disposition"
            ),
        ) as disposition_reducer:
            prepared = prepare_generation_operator_contract(
                comments=deepcopy(client.comments), graph=deepcopy(raw_graph),
                workstream_id="GEN-37", authority=AUTHORITY,
                description_plan_revision=predecessor_plan,
                target_source={
                    "identity": "https://example.test/new-plan", "sha256": PLAN,
                },
                created_at="2026-09-01T07:47:35Z", remote_head=HEAD,
                started_state={
                    "id": "started", "name": "In Progress",
                    "type": "started", "team_id": "team",
                },
            )
        disposition_reducer.assert_not_called()
        manifest = prepared["projection_preview"]["manifest"]
        self.assertEqual(prepared["projection_preview"]["phase"],
                         "terminal_evidence_seed")
        self.assertEqual(
            manifest["terminal_child_evidence_seed_predecessor"]
            ["projection_revision"],
            11,
        )
        self.assertIsNotNone(
            manifest["terminal_child_evidence_seed_predecessor"]
            ["checkpoint_event_id"],
        )

        projection_graph = workstream_projection.bind_projection_plan_generation(
            deepcopy(raw_graph), client.comments, workstream_id="GEN-37",
            requested_plan_revision=PLAN, authenticated_route=AUTHORITY,
        )
        projection_graph = add_live_child_material_history(
            projection_graph, authenticated_route=AUTHORITY,
            root_comments=client.comments,
            proposal_plan_revision=predecessor_plan,
        )
        target = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        normalized = prepare_terminal_child_evidence_seeds(
            manifest, projection_graph, target.state(), remote_head=HEAD,
            comments=client.comments,
        )
        self.assertEqual(normalized, manifest)

        with mock.patch.object(
            workstream_projection, "choose_disposition",
            return_value={
                "disposition": "create_successor",
                "recovered_from_checkpoint": None,
            },
        ):
            preview, unresolved = load_material_history_for_projection_reconcile(
                projection_graph, client.comments, "GEN-37", normalized, target,
                authenticated_route=AUTHORITY,
                authenticated_source={
                    "identity": "https://example.test/new-plan", "sha256": PLAN,
                },
                remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        readbacks = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        with mock.patch.object(
            workstream_projection, "projection_disposition_value",
            return_value={
                "disposition": "create_successor", "remote_head": HEAD,
                "recovered_from_checkpoint": None,
            },
        ):
            reconcile_required_projection(
                target, preview, normalized, remote_head=HEAD,
                created_at="2026-09-01T07:48:00Z",
                authenticated_source={
                    "identity": "https://example.test/new-plan", "sha256": PLAN,
                },
                relation_target_resolver=self.relation_target_resolver,
                terminal_child_fence=lambda child_ids: {
                    child_id: readbacks[child_id] for child_id in child_ids
                },
                projection_input_fence=lambda: manifest[
                    "terminal_child_evidence_seed_predecessor"
                ]["input_frontier_sha256"],
                checkpoint_fence=lambda: None,
                projection_comments=client.comments,
                projection_input_snapshot=projection_graph,
                legacy_unresolved_relation_heads=unresolved,
            )
        successor_head = "f" * 40
        with mock.patch.object(
            workstream_projection, "projection_disposition_value",
            return_value={
                "disposition": "create_successor",
                "remote_head": successor_head,
                "recovered_from_checkpoint": None,
            },
        ):
            successor = prepare_generation_operator_contract(
                comments=deepcopy(client.comments), graph=deepcopy(raw_graph),
                workstream_id="GEN-37", authority=AUTHORITY,
                description_plan_revision=predecessor_plan,
                target_source={
                    "identity": "https://example.test/new-plan", "sha256": PLAN,
                },
                created_at="2026-09-01T07:49:00Z", remote_head=successor_head,
                started_state={
                    "id": "started", "name": "In Progress",
                    "type": "started", "team_id": "team",
                },
            )
        successor_manifest = successor["projection_preview"]["manifest"]
        self.assertIn(
            "terminal_child_evidence_seed_head_transition", successor_manifest,
        )
        successor_scope = next(
            item["value"] for item in successor_manifest["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        self.assertEqual(
            next(
                repository for repository in successor_scope["repositories"]
                if repository_key(repository) == "github.com:id:R_pulp"
            ),
            pulp_repository,
        )
        self.assertEqual(
            successor_scope["child_ownership"],
            predecessor_scope["child_ownership"],
        )

        successor_graph = workstream_projection.bind_projection_plan_generation(
            deepcopy(raw_graph), client.comments, workstream_id="GEN-37",
            requested_plan_revision=PLAN, authenticated_route=AUTHORITY,
        )
        successor_graph = add_live_child_material_history(
            successor_graph, authenticated_route=AUTHORITY,
            root_comments=client.comments,
            proposal_plan_revision=predecessor_plan,
        )
        writes_before_negatives = len(client.comments)

        def successor_seed(candidate):
            return prepare_terminal_child_evidence_seeds(
                candidate, successor_graph, target.state(),
                remote_head=successor_head, comments=client.comments,
            )

        missing_binding = deepcopy(successor_manifest)
        missing_binding.pop("terminal_child_evidence_seed_predecessor")
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_nonprimary_predecessor_required:GEN-70",
        ):
            successor_seed(missing_binding)

        owner_substitution = deepcopy(successor_manifest)
        next(
            item for item in owner_substitution["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )["value"]["child_ownership"]["GEN-70"] = (
            predecessor_scope["primary_repository"]
        )
        with self.assertRaisesRegex(
            LinearProjectionError,
            "closure_history_repository_mismatch:GEN-70",
        ):
            successor_seed(owner_substitution)

        for field, value in (
            ("repository_key", predecessor_scope["primary_repository"]),
            ("exact_head", "8" * 40),
        ):
            with self.subTest(contract_substitution=field):
                changed = deepcopy(successor_manifest)
                next(
                    item for item in changed["projection"]
                    if item["kind"] == "evidence_contract"
                )["value"][field] = value
                with self.assertRaisesRegex(
                    LinearProjectionError,
                    "terminal_seed_predecessor_contract_mutated:GEN-70",
                ):
                    successor_seed(changed)

        closure_digest = deepcopy(successor_manifest)
        closure_digest["terminal_child_evidence_seed_predecessor"][
            "evidence_heads"
        ][0]["closure_value_sha256"] = "0" * 64
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_seed_predecessor_binding_changed_reload_required",
        ):
            successor_seed(closure_digest)

        future_secondary = deepcopy(successor_manifest)
        future_scope = next(
            item["value"] for item in future_secondary["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        next(
            repository for repository in future_scope["repositories"]
            if repository_key(repository) == "github.com:id:R_pulp"
        )["exact_head"] = "6" * 40
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_scope_head_only_required",
        ):
            successor_seed(future_secondary)
        self.assertEqual(len(client.comments), writes_before_negatives)

        base_comments = deepcopy(client.comments)
        source = {
            "identity": "https://example.test/new-plan", "sha256": PLAN,
        }

        def transition_case():
            transition_client = FakeProjectionClient()
            transition_client.comments = deepcopy(base_comments)
            transition_adapter = LinearProjectionAdapter(
                transition_client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=PLAN, **AUTHORITY,
            )
            return transition_client, transition_adapter

        def apply_transition(
            transition_client, transition_adapter, *, interrupt_after=None,
        ):
            graph = workstream_projection.bind_projection_plan_generation(
                deepcopy(raw_graph), transition_client.comments,
                workstream_id="GEN-37", requested_plan_revision=PLAN,
                authenticated_route=AUTHORITY,
            )
            graph = add_live_child_material_history(
                graph, authenticated_route=AUTHORITY,
                root_comments=transition_client.comments,
                proposal_plan_revision=predecessor_plan,
            )
            reviewed = prepare_terminal_child_evidence_seeds(
                successor_manifest, graph, transition_adapter.state(),
                remote_head=successor_head,
                comments=transition_client.comments,
            )
            disposition_value = reviewed[
                "terminal_child_evidence_seed_head_transition"
            ]["disposition"]
            with mock.patch.object(
                workstream_projection, "choose_disposition",
                return_value={
                    "disposition": disposition_value["disposition"],
                    "recovered_from_checkpoint": disposition_value[
                        "recovered_from_checkpoint"
                    ],
                },
            ):
                preview, unresolved = load_material_history_for_projection_reconcile(
                    graph, transition_client.comments, "GEN-37", reviewed,
                    transition_adapter, authenticated_route=AUTHORITY,
                    authenticated_source=source, remote_head=successor_head,
                    relation_target_resolver=self.relation_target_resolver,
                )
            original_execute = transition_client.execute
            append_count = 0

            def interrupted_execute(query, variables):
                nonlocal append_count
                response = original_execute(query, variables)
                if "commentCreate" in query:
                    append_count += 1
                    if append_count == interrupt_after:
                        raise SystemExit("simulated successor caller death")
                return response

            if interrupt_after is not None:
                transition_client.execute = interrupted_execute
            try:
                with mock.patch.object(
                    workstream_projection, "projection_disposition_value",
                    return_value=deepcopy(disposition_value),
                ):
                    return reconcile_required_projection(
                        transition_adapter, preview, reviewed,
                        remote_head=successor_head,
                        created_at="2026-09-01T07:50:00Z",
                        authenticated_source=source,
                        relation_target_resolver=self.relation_target_resolver,
                        terminal_child_fence=lambda child_ids: {
                            child_id: readbacks[child_id]
                            for child_id in child_ids
                        },
                        projection_input_fence=lambda: reviewed[
                            "terminal_child_evidence_seed_head_transition"
                        ]["input_frontier_sha256"],
                        checkpoint_fence=lambda: None,
                        projection_comments=transition_client.comments,
                        projection_input_snapshot=graph,
                        legacy_unresolved_relation_heads=unresolved,
                    )
            finally:
                transition_client.execute = original_execute

        clean_client, clean_adapter = transition_case()
        clean_result = apply_transition(clean_client, clean_adapter)
        canonical_write_count = len(clean_result["writes"])
        self.assertGreater(canonical_write_count, 0)
        for prefix in range(1, canonical_write_count + 1):
            with self.subTest(successor_crash_prefix=prefix):
                crash_client, crash_adapter = transition_case()
                with self.assertRaisesRegex(
                    SystemExit, "simulated successor caller death",
                ):
                    apply_transition(
                        crash_client, crash_adapter, interrupt_after=prefix,
                    )
                apply_transition(crash_client, crash_adapter)
                active = workstream_projection._active_heads(
                    crash_adapter.state()
                )
                final_scope = active[("scope", "root")]["value"]
                self.assertEqual(
                    final_scope["child_ownership"],
                    predecessor_scope["child_ownership"],
                )
                self.assertEqual(
                    next(
                        repository
                        for repository in final_scope["repositories"]
                        if repository_key(repository) == "github.com:id:R_pulp"
                    ),
                    pulp_repository,
                )
                final_contract = active[(
                    "evidence_contract", "gen-70-terminal-receipts-v1",
                )]["value"]
                self.assertEqual(final_contract["exact_head"], "9" * 40)
                self.assertEqual(
                    final_contract["predecessor_closure_authority"],
                    next(
                        item["value"]["predecessor_closure_authority"]
                        for item in successor_manifest["projection"]
                        if item["kind"] == "evidence_contract"
                    ),
                )
                replay_writes = len(crash_client.comments)
                replay = apply_transition(crash_client, crash_adapter)
                self.assertEqual(replay["writes"], [])
                self.assertEqual(len(crash_client.comments), replay_writes)

    def install_late_predecessor_projection_append(
        self, client, adapter, *, after_append, suffix,
    ):
        predecessor_plan = "b" * 64
        generation = sorted(
            [
                event for event in adapter.state().snapshot["projection_history"]
                if event["plan_revision"] == predecessor_plan
            ],
            key=lambda event: event["expected_revision"],
        )
        late = build_projection_event(
            workstream_id="GEN-37", kind="provenance",
            key=f"late-{suffix}",
            value={
                "agent": "codex", "machine": "M5",
                "session_id": f"late-{suffix}",
                "worktree": {"state": "safe", "head": "2" * 40},
            },
            plan_revision=predecessor_plan,
            expected_revision=len(generation),
            created_at=f"2026-08-29T23:{suffix:02d}:00Z",
            authority=AUTHORITY,
        )
        original_execute = client.execute
        append_count = 0

        def execute(query, variables):
            nonlocal append_count
            result = original_execute(query, variables)
            if "commentCreate" in query:
                append_count += 1
                if append_count == after_append:
                    client.comments.append({
                        "id": f"late-predecessor-{suffix}",
                        "body": encode_projection_comment(late),
                        "createdAt": "2026-08-29T23:59:00Z",
                        "updatedAt": "2026-08-29T23:59:00Z",
                    })
            return result

        client.execute = execute

    def test_generation_carry_accepts_five_historical_and_one_current_head(self):
        client, current, _source, graph, _children, manifest, binding = (
            self.mixed_head_plan_generation_fixture(current_secondary=True)
        )
        self.assertEqual(
            [item["child_identifier"] for item in binding["evidence_heads"]],
            ["GEN-38", "GEN-39", "GEN-40", "GEN-41", "GEN-42", "GEN-85"],
        )
        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, current.state(), remote_head=HEAD,
            comments=client.comments,
        )
        self.assertEqual(
            prepared["terminal_child_evidence_seed_predecessor"], binding,
        )
        self.assertEqual(
            sum(
                "predecessor_closure_authority" in item["value"]
                for item in prepared["projection"]
                if item["kind"] == "evidence_contract"
            ),
            6,
        )

        predecessor_plan = "b" * 64
        scope_value = next(
            item["value"] for item in manifest["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        contracts = {
            item["key"]: item["value"] for item in manifest["projection"]
            if item["kind"] == "evidence_contract"
        }
        seeds = manifest["terminal_child_evidence_seeds"]

        def contract(candidate_graph, candidate_contracts=contracts):
            return terminal_child_evidence_seed_predecessor_contract(
                candidate_graph, current.state(), client.comments,
                workstream_id="GEN-37",
                predecessor_plan_revision=predecessor_plan,
                desired_scope=scope_value, seeds=seeds,
                desired_contracts=candidate_contracts,
            )

        duplicate = deepcopy(graph)
        duplicate["children"].append(next(
            deepcopy(child) for child in graph["children"]
            if child["identifier"] == "GEN-42"
        ))
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_seed_predecessor_child_ambiguous:GEN-42",
        ):
            contract(duplicate)

        stale = deepcopy(graph)
        next(
            child for child in stale["children"]
            if child["identifier"] == "GEN-42"
        )["assignee"] = {"id": "different-owner"}
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_seed_predecessor_evidence_not_authorized:GEN-42",
        ):
            contract(stale)

        wrong_head = deepcopy(contracts)
        wrong_head["gen-42-terminal"]["exact_head"] = "0" * 40
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_seed_predecessor_contract_mutated:GEN-42",
        ):
            contract(graph, wrong_head)

        (reconcile_client, reconcile_current, reconcile_source,
         reconcile_graph, _reconcile_children, reconcile_manifest,
         reconcile_binding) = self.mixed_head_plan_generation_fixture(
             current_secondary=True,
         )
        result = self.run_mixed_head_seed_reconcile(
            reconcile_client, reconcile_current, reconcile_source,
            reconcile_graph, reconcile_manifest, reconcile_binding,
        )
        self.assertFalse(result["resume_authority_verified"])
        self.assertEqual(result["projection_revision"], 10)
        self.assertEqual(len(result["writes"]), 10)

    def test_decoded_receipts_compute_predecessor_contract_without_writes(self):
        client, current, _source, graph, _children, manifest, binding = (
            self.mixed_head_plan_generation_fixture()
        )
        scope = next(
            item["value"] for item in manifest["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        contracts = {
            item["key"]: item["value"] for item in manifest["projection"]
            if item["kind"] == "evidence_contract"
        }
        writes_before = sum(
            "mutation " in query for query, _variables in client.calls
        )
        decoded = current.state()
        computed, _authorities = terminal_child_evidence_seed_predecessor_contract(
            graph, decoded, deepcopy(client.comments), workstream_id="GEN-37",
            predecessor_plan_revision="b" * 64, desired_scope=scope,
            seeds=manifest["terminal_child_evidence_seeds"],
            desired_contracts=contracts,
        )
        self.assertEqual(computed, binding)
        self.assertTrue(all(
            {"id", "body"} <= set(comment) for comment in client.comments
        ))
        self.assertEqual(sum(
            "mutation " in query for query, _variables in client.calls
        ), writes_before)

    def run_mixed_head_seed_reconcile(
        self, client, adapter, source, graph, manifest, binding,
    ):
        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=HEAD,
            comments=client.comments,
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        readbacks = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        return reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-29T23:30:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: readbacks[child_id] for child_id in child_ids
            },
            projection_input_fence=lambda: binding["input_frontier_sha256"],
            checkpoint_fence=lambda: None,
            projection_comments=client.comments,
            projection_input_snapshot=graph,
            legacy_unresolved_relation_heads=unresolved,
        )

    def mixed_head_repair_reconcile_fixture(self, *, activation_ready=False):
        client, adapter, source, graph, children, manifest, binding = (
            self.mixed_head_plan_generation_fixture(
                activation_ready=activation_ready,
            )
        )
        self.run_mixed_head_seed_reconcile(
            client, adapter, source, graph, manifest, binding,
        )
        active = workstream_projection._active_heads(adapter.state())
        readbacks = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        repairs = []
        for child in children:
            event = next(
                event for (kind, _key), event in active.items()
                if kind == "evidence_contract"
                and event["value"]["owning_child"] == child["identifier"]
            )
            repairs.append({
                "child_identifier": child["identifier"],
                "child_issue_id": child["id"],
                "expected_child_readback_sha256": readbacks[
                    child["identifier"]
                ],
                "expected_assignee_id": child["assignee"]["id"],
                "approved_evidence_heads": [{
                    "key": event["key"], "event_id": event["event_id"],
                    "value_sha256": canonical_digest(event["value"]),
                }],
            })
        projection = [
            {"kind": kind, "key": key, "value": deepcopy(event["value"])}
            for (kind, key), event in sorted(active.items())
            if kind != "disposition"
        ]
        repair_manifest = {
            **reviewed_manifest(adapter, projection),
            "terminal_child_repairs": repairs,
        }
        prepared = prepare_terminal_child_repairs(
            repair_manifest, graph, adapter.state(),
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        return (
            client, adapter, source, graph, prepared, binding, readbacks,
            preview, unresolved,
        )

    def run_mixed_head_repair_reconcile(self, fixture):
        (
            _client, adapter, source, _graph, prepared, binding, readbacks,
            preview, unresolved,
        ) = fixture
        return reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-29T23:45:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: readbacks[child_id] for child_id in child_ids
            },
            projection_input_fence=lambda: binding["input_frontier_sha256"],
            checkpoint_fence=lambda: None,
            legacy_unresolved_relation_heads=unresolved,
        )

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

    def multi_terminal_repair_fixture(
        self, *, evidence_active=True, unassigned_child=None,
    ):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        children = []
        contracts = []
        for identifier, issue_id, assignee_id, state_id in (
            ("GEN-70", "70707070-7070-4070-8070-707070707070",
             "80808080-8080-4080-8080-808080808080",
             "90909090-9090-4090-8090-909090909090"),
            ("GEN-72", "72727272-7272-4272-8272-727272727272",
             "82828282-8282-4282-8282-828282828282",
             "92929292-9292-4292-8292-929292929292"),
        ):
            contract = evidence_contract()
            contract["slice_id"] = f"{identifier.lower()}-terminal"
            contract["owning_child"] = identifier
            contracts.append(contract)
            children.append({
                "id": issue_id, "identifier": identifier,
                "title": f"terminal child {identifier}",
                "parent": {"id": ROOT_UUID, "identifier": "GEN-37"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                "assignee": {"id": assignee_id},
                "state_id": state_id,
                "status": "Done", "status_type": "completed",
            })
        if unassigned_child is not None:
            next(
                child for child in children
                if child["identifier"] == unassigned_child
            )["assignee"] = None
        owned_scope = scope()
        for child in children:
            owned_scope["child_ownership"][child["identifier"]] = (
                "github.com:id:R_agent_workstream"
            )
        base_without_evidence = [
            {"kind": "scope", "key": "root", "value": owned_scope},
            {"kind": "source", "key": "root", "value": source},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
                "worktree": {"state": "safe", "head": HEAD},
            }},
        ]
        evidence_items = [
            {"kind": "evidence_contract", "key": contract["slice_id"],
             "value": contract}
            for contract in contracts
        ]
        base = [*base_without_evidence, *evidence_items]
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(
                adapter, base if evidence_active else base_without_evidence,
            ), remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
        )
        graph = self.graph_snapshot()
        graph["children"] = [
            {"identifier": "GEN-38", "title": "existing",
             "status": "In Progress", "next_action": "continue"},
            *children,
        ]
        if not evidence_active:
            seeds = [
                {
                    "child_identifier": child["identifier"],
                    "child_issue_id": child["id"],
                    "expected_child_readback_sha256": canonical_digest(
                        terminal_child_readback(child)
                    ),
                    "expected_assignee_id": (
                        child.get("assignee") or {}
                    ).get("id"),
                    "evidence_keys": [
                        contract["slice_id"] for contract in contracts
                        if contract["owning_child"] == child["identifier"]
                    ],
                }
                for child in children
            ]
            return client, adapter, source, graph, children, {
                **reviewed_manifest(adapter, deepcopy(base)),
                "terminal_child_evidence_seeds": seeds,
            }
        evidence_events = {
            event["value"]["owning_child"]: event
            for event in adapter.state().events
            if event["kind"] == "evidence_contract"
        }
        repairs = []
        for child in children:
            event = evidence_events[child["identifier"]]
            repairs.append({
                "child_identifier": child["identifier"],
                "child_issue_id": child["id"],
                "expected_child_readback_sha256": canonical_digest(
                    terminal_child_readback(child)
                ),
                "expected_assignee_id": (
                    child.get("assignee") or {}
                ).get("id"),
                "approved_evidence_heads": [{
                    "key": event["key"], "event_id": event["event_id"],
                    "value_sha256": canonical_digest(event["value"]),
                }],
            })
        manifest = {
            **reviewed_manifest(adapter, deepcopy(base)),
            "terminal_child_repairs": repairs,
        }
        return client, adapter, source, graph, children, manifest

    def gen37_production_shaped_fixture(self, *, stale_child_history=False):
        """Build five closures plus two verbose open children and checkpoints."""
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        identifiers = [f"GEN-{number}" for number in range(38, 43)]
        open_identifiers = ["GEN-43", "GEN-85"]
        owned_scope = scope()
        owned_scope["child_ownership"] = {
            identifier: "github.com:id:R_agent_workstream"
            for identifier in [*identifiers, *open_identifiers]
        }
        children = []
        contracts = []
        for number, identifier in enumerate(identifiers, start=38):
            contract = evidence_contract()
            contract["slice_id"] = f"{identifier.lower()}-terminal"
            contract["owning_child"] = identifier
            for layer in contract["layers"].values():
                for receipt in layer.get("receipts", []):
                    receipt["proof"] += ":" + (identifier + "-proof-") * 180
            contracts.append(contract)
            children.append({
                "id": f"child-{number}", "identifier": identifier,
                "title": f"terminal child {identifier}",
                "parent": {"id": ROOT_UUID, "identifier": "GEN-37"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                "assignee": {"id": f"assignee-{number}"},
                "state_id": f"done-state-{number}",
                "status": "Done", "status_type": "completed",
            })
        base = [
            {"kind": "scope", "key": "root", "value": owned_scope},
            {"kind": "source", "key": "root", "value": source},
            *[
                {"kind": "provenance", "key": f"session-{index}", "value": {
                    "agent": "codex", "machine": machine,
                    "session_id": f"session-{index}",
                    **({"worktree": {
                        "state": "safe", "head": HEAD,
                        "path": f"/worktrees/gen37-session-{index}",
                        "branch": f"fix/gen37-session-{index}",
                    }} if index == 4 else {}),
                }}
                for index, machine in enumerate(("M1", "M3", "M5", "M3", "M5"))
            ],
            *[
                {"kind": "evidence_contract", "key": contract["slice_id"],
                 "value": contract}
                for contract in contracts
            ],
        ]
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, base), remote_head=HEAD,
            created_at="2026-08-29T18:00:00Z", authenticated_source=source,
        )
        graph = self.graph_snapshot()
        open_children = [
            {
                "id": "open-child-43", "identifier": "GEN-43",
                "title": "Durable launch continuation", "url": "https://linear/GEN-43",
                "description": "Verbose launch history. " * 90,
                "parent": {"id": ROOT_UUID, "identifier": "GEN-37"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"}, "assignee": {"id": "owner-43"},
                "state_id": "started", "status": "In Progress",
                "status_type": "started", "next_action": "stale issue action",
                "updatedAt": "2026-08-29T18:00:00Z",
            },
            {
                "id": "open-child-85", "identifier": "GEN-85",
                "title": "Aggregate child recovery", "url": "https://linear/GEN-85",
                "description": "Verbose aggregation requirements. " * 30,
                "parent": {"id": ROOT_UUID, "identifier": "GEN-37"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"}, "assignee": {"id": "owner-85"},
                "state_id": "started", "status": "In Progress",
                "status_type": "started", "next_action": "Implement aggregation.",
                "updatedAt": "2026-08-29T18:00:00Z",
            },
        ]
        graph["children"] = [*open_children, *children]
        child_events = [Delta(
            "gen43-progress", "GEN-43", "progress", "agent",
            {"next_action": "Land the M3 adapter, then rerun the continuation canary."},
            0, "2026-08-29T18:10:00Z",
        )]
        if stale_child_history:
            obligation_kinds = (
                "blocker", "decision", "decision_required", "followup",
                "requirement",
            )
            child_events.extend(
                Delta(
                    f"gen43-obligation-{index}", "GEN-43",
                    obligation_kinds[index % len(obligation_kinds)], "agent",
                    {
                        obligation_kinds[index % len(obligation_kinds)]:
                        f"historical obligation {index}: " + "x" * 420,
                    },
                    index + 1, f"2026-08-29T18:{index + 11:02d}:00Z",
                )
                for index in range(30)
            )
        child_checkpoint = build_checkpoint(
            workstream_id="GEN-43", boundary_id="gen43-current",
            root_revision=len(child_events),
            plan_revision=("f" * 64 if stale_child_history else PLAN),
            before_status="In Progress",
            after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session-child",
                "machine": "M5", "worktree": {"state": "unavailable"},
            },
            exact_head=None,
            evidence=[{"kind": "focused", "proof": "e" * 500} for _ in range(3)],
            blocker={"owner_machine": "M3", "reason": "M3 is the sole writer."},
            next_action="Land the M3 adapter, then rerun the continuation canary.",
        )
        graph = add_child_material_history(
            graph,
            {
                "GEN-43": [
                    *[
                        {"id": f"gen43-event-{index}",
                         "body": encode_event_comment(event)}
                        for index, event in enumerate(child_events)
                    ],
                    {"id": "gen43-checkpoint",
                     "body": encode_checkpoint_comment(child_checkpoint)},
                ],
                "GEN-85": [],
            },
            authenticated_route=AUTHORITY,
        )
        root_event = Delta(
            "gen37-progress", "GEN-37", "progress", "agent",
            {"next_action": "Finish both open children, then run physical acceptance."},
            0, "2026-08-29T18:20:00Z",
        )
        root_checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="gen37-current", root_revision=1,
            plan_revision=PLAN, before_status="In Progress",
            after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session-root",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/worktrees/gen37", "branch": "fix/gen37",
                    "head": HEAD,
                },
            },
            exact_head=HEAD,
            evidence=[{"kind": "focused", "proof": "r" * 500} for _ in range(5)],
            blocker={"conditions": ["GEN-43 remains open", "GEN-85 remains open"]},
            next_action="Finish both open children, then run physical acceptance.",
        )
        client.comments.extend([
            {"id": "gen37-event", "body": encode_event_comment(root_event)},
            {"id": "gen37-checkpoint", "body": encode_checkpoint_comment(root_checkpoint)},
        ])
        evidence_events = {
            event["value"]["owning_child"]: event
            for event in adapter.state().events
            if event["kind"] == "evidence_contract"
        }
        repairs = []
        for child in children:
            event = evidence_events[child["identifier"]]
            repairs.append({
                "child_identifier": child["identifier"],
                "child_issue_id": child["id"],
                "expected_child_readback_sha256": canonical_digest(
                    terminal_child_readback(child)
                ),
                "expected_assignee_id": child["assignee"]["id"],
                "approved_evidence_heads": [{
                    "key": event["key"], "event_id": event["event_id"],
                    "value_sha256": canonical_digest(event["value"]),
                }],
            })
        manifest = {
            **reviewed_manifest(adapter, deepcopy(base)),
            "terminal_child_repairs": repairs,
        }
        prepared = prepare_terminal_child_repairs(
            manifest, graph, adapter.state(),
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = {
            repair["child_identifier"]: repair["expected_child_readback_sha256"]
            for repair in repairs
        }
        reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-29T19:00:00Z", authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected[child_id] for child_id in child_ids
            },
            checkpoint_fence=lambda: preview["latest_checkpoint"][
                "checkpoint_event_id"
            ],
            legacy_unresolved_relation_heads=unresolved,
        )
        strict = add_material_history(
            graph, client.comments, "GEN-37", authenticated_route=AUTHORITY,
            authenticated_source=source,
        )
        empty_sha = hashlib.sha256(b"[]").hexdigest()
        strict["dependency_graph"] = {
            "schema_version": 1,
            "authority": "child_dependency_authorization",
            "plan_revision": PLAN,
            "route": deepcopy(AUTHORITY),
            "revision": 0,
            "sha256": empty_sha,
            "authorization_batches": [],
            "relations": [],
            "native_readback": "relations_and_inverseRelations",
            "ignored_non_dependency_count": 0,
            "observed_frontier": {
                "material_revision": strict["material_event_revision"],
                "projection_revision": strict["projection_revision"],
                "graph_revision": 0,
                "graph_sha256": empty_sha,
            },
            "root_readback_sha256": dependency_root_readback_sha256(strict["root"]),
        }
        return strict, contracts

    def test_gen37_stale_generation_child_history_is_digest_bound_and_actionable(self):
        strict, _contracts = self.gen37_production_shaped_fixture(
            stale_child_history=True,
        )
        context = compact_context(
            strict, "GEN-37", require_projection_authority=True,
        )
        encoded = json.dumps(
            context, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()
        child = next(
            item for item in context["children"]
            if item["identifier"] == "GEN-43"
        )

        self.assertLessEqual(len(encoded), 24 * 1024)
        self.assertEqual(context["resume_authority"], "full")
        self.assertEqual(child["status"], "In Progress")
        self.assertEqual(
            child["next_action"],
            "Land the M3 adapter, then rerun the continuation canary.",
        )
        self.assertEqual(child["checkpoint_recovery"]["state"], "stale_plan")
        self.assertEqual(
            child["stale_plan_material_obligations"]["checkpoint_root_revision"],
            31,
        )
        self.assertEqual(
            child["stale_plan_material_obligations"]["acknowledged_count"], 30,
        )
        self.assertEqual(
            child["stale_plan_material_obligations"]["uncheckpointed_count"], 0,
        )
        self.assertRegex(
            child["stale_plan_material_obligations"]["sha256"], r"^[0-9a-f]{64}$",
        )
        self.assertEqual(child["uncheckpointed_material_obligations"], [])
        self.assertEqual(len(context["child_closures"]), 5)
        self.assertEqual(len(context["evidence_contracts"]), 5)
        self.assertEqual(context["source"]["sha256"], PLAN)
        self.assertEqual(context["disposition"]["disposition"], "attach")
        self.assertEqual(context["latest_checkpoint"]["plan_revision"], PLAN)
        self.assertEqual(context["provenance"]["worktree_authority_count"], 1)
        self.assertEqual(context["projection_quarantine"]["count"], 0)

    def test_gen37_production_shaped_resume_has_meaningful_budget_headroom(self):
        strict, contracts = self.gen37_production_shaped_fixture()
        context = compact_context(
            strict, "GEN-37", max_bytes=16 * 1024, max_items=100,
            require_projection_authority=True,
        )
        encoded = json.dumps(
            context, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()
        self.assertLessEqual(len(encoded), int(14.5 * 1024))
        self.assertEqual(context["resume_authority"], "full")
        self.assertEqual(context["context_schema"], {
            "name": "agent-workstream.resume-context", "version": 2,
            "representation": "compact_validated",
        })
        self.assertEqual(context["provenance"]["worktree_authority_count"], 1)
        self.assertFalse(context["provenance"]["worktree_authority_ambiguous"])
        self.assertEqual(
            [child["identifier"] for child in context["children"]],
            ["GEN-43", "GEN-85"],
        )
        self.assertEqual(
            context["children"][0]["next_action"],
            "Land the M3 adapter, then rerun the continuation canary.",
        )
        self.assertEqual(
            context["children"][0]["blocker"],
            {"owner_machine": "M3", "reason": "M3 is the sole writer."},
        )
        self.assertNotIn("description", context["children"][0])
        self.assertRegex(
            context["children"][0]["description_summary"]["sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertEqual(len(context["child_closures"]), 5)
        self.assertEqual(len(context["evidence_contracts"]), 5)
        for closure in context["child_closures"]:
            self.assertNotIn("child_issue_id", closure)
            self.assertNotIn("evidence_heads", closure)
            self.assertIn("assignee_id", closure)
            self.assertEqual(closure["state_name"], "Done")
            self.assertEqual(closure["state_type"], "completed")
            self.assertEqual(closure["evidence_head_count"], 1)
            self.assertRegex(closure["evidence_heads_sha256"], r"^[0-9a-f]{64}$")
            self.assertRegex(
                closure["projection_head"]["value_sha256"], r"^[0-9a-f]{64}$",
            )
        for compact, complete in zip(context["evidence_contracts"], contracts):
            self.assertNotIn("layers", compact)
            self.assertEqual(compact["contract_sha256"], canonical_digest(complete))
            self.assertEqual(compact["exact_head"], HEAD)
            self.assertEqual(compact["receipt_count"], 5)
            self.assertTrue(evidence_errors(compact))
            self.assertEqual(
                compact["projection_head"]["key"], complete["slice_id"],
            )
            self.assertEqual(
                compact["projection_head"]["value_sha256"],
                compact["contract_sha256"],
            )
            self.assertRegex(
                compact["projection_head"]["event_id"],
                r"^wsp_[0-9a-f]{32}$",
            )

    def test_projection_budget_preflight_includes_child_history_and_checkpoint_relief(self):
        """A GEN-37-like root-only preview must not undercount open children."""
        strict, _contracts = self.gen37_production_shaped_fixture()
        raw = deepcopy(strict)
        for child in raw["children"]:
            if child["identifier"] not in {"GEN-43", "GEN-85"}:
                continue
            for key in (
                "issue_next_action", "material_events", "material_event_revision",
                "checkpoint_history", "latest_checkpoint", "checkpoint_recovery",
                "blocker",
            ):
                child.pop(key, None)
            child["next_action"] = "Continue the open child."

        events = [
            Delta(
                f"gen43-uncheckpointed-{index}", "GEN-43", "requirement",
                "agent", {"requirement": f"Requirement {index}: " + "x" * 900},
                index, f"2026-08-29T20:{index:02d}:00Z",
            )
            for index in range(16)
        ]
        event_comments = [
            {"id": f"gen43-event-{index}", "body": encode_event_comment(event)}
            for index, event in enumerate(events)
        ]
        raw["child_comments"] = {"GEN-43": event_comments, "GEN-85": []}

        root_only = deepcopy(raw)
        root_only.pop("child_comments")
        root_context = compact_context(
            root_only, "GEN-37", max_bytes=1024 * 1024, max_items=500,
            require_projection_authority=True,
        )
        child_aware = add_live_child_material_history(
            raw, authenticated_route=AUTHORITY, root_comments=[],
        )
        child_context = compact_context(
            child_aware, "GEN-37", max_bytes=1024 * 1024, max_items=500,
            require_projection_authority=True,
        )
        encode = lambda value: json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()
        root_bytes = len(encode(root_context))
        child_bytes = len(encode(child_context))
        cap = (root_bytes + child_bytes) // 2
        self.assertLess(root_bytes, cap)
        self.assertGreater(child_bytes, cap)

        comments_before = deepcopy(raw["child_comments"])
        with self.assertRaisesRegex(ResumeError, "resume_context_over_budget"):
            compact_context(
                child_aware, "GEN-37", max_bytes=cap, max_items=500,
                require_projection_authority=True,
            )
        self.assertEqual(raw["child_comments"], comments_before)

        checkpoint = build_checkpoint(
            workstream_id="GEN-43", boundary_id="gen43-after-requirements",
            root_revision=len(events), plan_revision=PLAN,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "session-after-checkpoint", "machine": "M5",
                "worktree": {"state": "unavailable"},
            },
            exact_head=None, evidence=[], blocker=None,
            next_action="Implement the checkpointed requirements.",
        )
        checkpointed = deepcopy(raw)
        checkpointed["child_comments"]["GEN-43"].append({
            "id": "gen43-checkpoint-after-requirements",
            "body": encode_checkpoint_comment(checkpoint),
        })
        checkpointed = add_live_child_material_history(
            checkpointed, authenticated_route=AUTHORITY, root_comments=[],
        )
        resumed = compact_context(
            checkpointed, "GEN-37", max_bytes=cap, max_items=500,
            require_projection_authority=True,
        )
        self.assertEqual(resumed["resume_authority"], "full")
        self.assertLessEqual(len(encode(resumed)), cap)

    def test_compact_resume_builds_launch_profile_without_authority_rehydration(self):
        strict, _contracts = self.gen37_production_shaped_fixture()
        empty_sha = hashlib.sha256(b"[]").hexdigest()
        strict["dependency_graph"] = {
            "schema_version": 1,
            "authority": "child_dependency_authorization",
            "plan_revision": PLAN,
            "route": deepcopy(AUTHORITY),
            "revision": 0,
            "sha256": empty_sha,
            "authorization_batches": [],
            "relations": [],
            "native_readback": "relations_and_inverseRelations",
            "ignored_non_dependency_count": 0,
            "observed_frontier": {
                "material_revision": strict["material_event_revision"],
                "projection_revision": strict["projection_revision"],
                "graph_revision": 0,
                "graph_sha256": empty_sha,
            },
            "root_readback_sha256": dependency_root_readback_sha256(strict["root"]),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            worktree = strict["latest_checkpoint"]["worktree"]
            worktree["path"] = str(root)
            strict["latest_checkpoint"]["provenance_chain"][-1][
                "worktree"
            ] = deepcopy(worktree)
            context = compact_context(
                strict, "GEN-37", max_bytes=16 * 1024, max_items=100,
                require_projection_authority=True,
            )
            checkpoint = context["latest_checkpoint"]
            self.assertEqual(checkpoint["workstream_id"], "GEN-37")
            self.assertEqual(checkpoint["plan_revision"], PLAN)
            self.assertEqual(
                checkpoint["next_action"], context["next_action"],
            )
            self.assertEqual(
                checkpoint["provenance"]["latest"]["event_id"],
                checkpoint["checkpoint_event_id"],
            )
            profile = shipyard_profile.build_launch_profile(
                context, "GEN-37", shipyard_profile.GitIdentity(
                    root=root,
                    repository_coordinate=(
                        "github.com/generous-corp/agent-workstream"
                    ),
                    repository="generous-corp/agent-workstream",
                    head=HEAD, branch="fix/gen37",
                ),
                model="gpt-5.6-sol", reasoning_effort="medium",
            )
            self.assertEqual(
                profile["checkpoint"]["checkpoint_id"],
                checkpoint["checkpoint_event_id"],
            )

    def test_full_resume_refuses_stale_checkpoint_disposition_but_inspection_does_not_write(self):
        strict, _contracts = self.gen37_production_shaped_fixture()
        stale = deepcopy(strict)
        current = next(
            event for event in reversed(stale["projection_events"])
            if (event["kind"], event["key"]) == ("disposition", "root")
        )
        stale_disposition = {
            **stale["disposition"],
            "recovered_from_checkpoint": "wsc_" + "e" * 32,
        }
        event = build_projection_event(
            workstream_id="GEN-37", kind="disposition", key="root",
            value=stale_disposition, plan_revision=PLAN,
            expected_revision=stale["projection_revision"],
            created_at="2026-08-29T19:01:00Z",
            supersedes_event_id=current["event_id"], authority=AUTHORITY,
        )
        stale["projection_events"].append(event)
        stale["projection_revision"] += 1
        stale["disposition"] = stale_disposition
        with self.assertRaisesRegex(
            ResumeError,
            "disposition_checkpoint_stale_reconcile_required",
        ):
            compact_context(
                stale, "GEN-37", require_projection_authority=True,
            )
        inspection_snapshot = deepcopy(stale)
        inspection_snapshot.pop("dependency_graph")
        inspected = compact_context(
            inspection_snapshot, "GEN-37", require_projection_authority=False,
        )
        self.assertEqual(inspected["resume_authority"], "inspection_only")
        self.assertEqual(inspected["disposition"], stale_disposition)

    def test_explicit_history_retains_full_verbose_evidence_contracts(self):
        strict, contracts = self.gen37_production_shaped_fixture()
        with self.assertRaisesRegex(ResumeError, "resume_context_over_budget"):
            compact_context(
                strict, "GEN-37", max_bytes=16 * 1024, max_items=500,
                require_projection_authority=True, include_history=True,
            )
        full = compact_context(
            strict, "GEN-37", max_bytes=1024 * 1024, max_items=500,
            require_projection_authority=True, include_history=True,
        )
        self.assertEqual(full["evidence_contracts"], contracts)
        self.assertEqual(full["context_schema"]["representation"], "full_validated")
        self.assertIn("layers", full["evidence_contracts"][0])
        self.assertIsInstance(full["provenance"], list)
        self.assertIn("description", full["children"][0])
        self.assertEqual(len(full["children"][0]["latest_checkpoint"]["evidence"]), 3)
        self.assertIn("child_issue_id", full["child_closures"][0])
        self.assertIn("evidence_heads", full["child_closures"][0])

    def test_compaction_does_not_weaken_contract_tamper_validation(self):
        strict, _contracts = self.gen37_production_shaped_fixture()
        tampered = deepcopy(strict)
        tampered["evidence_contracts"][0]["layers"]["logic"]["receipts"][0][
            "proof"
        ] = "tampered after authenticated projection"
        with self.assertRaisesRegex(
            ResumeError, "projection_current_view_mismatch:evidence_contracts",
        ):
            compact_context(
                tampered, "GEN-37", max_bytes=16 * 1024, max_items=100,
                require_projection_authority=True,
            )

    def test_authoritative_compaction_requires_exact_projection_heads(self):
        strict, _contracts = self.gen37_production_shaped_fixture()
        with mock.patch.object(
            resume_module, "_projection_head_for_value", return_value=None,
        ), self.assertRaisesRegex(
            ResumeError, "evidence_compaction_projection_head_missing",
        ):
            compact_context(
                strict, "GEN-37", max_bytes=16 * 1024, max_items=100,
                require_projection_authority=True,
            )
        with self.assertRaisesRegex(
            ResumeError, "child_closure_compaction_projection_head_missing",
        ):
            resume_module._compact_child_closures(
                strict["child_closures"], [], require_projection_authority=True,
            )

    def test_evidence_key_must_equal_slice_id(self):
        contract = evidence_contract()
        with self.assertRaisesRegex(
            LinearProjectionError, "projection_evidence_key_mismatch",
        ):
            build_projection_event(
                workstream_id="GEN-37", kind="evidence_contract",
                key="different-projection-key", value=contract,
                plan_revision=PLAN, expected_revision=0,
                created_at="2026-08-29T20:00:00Z", authority=AUTHORITY,
            )

    def test_closure_unknown_field_and_digest_mutation_refuse(self):
        strict, _contracts = self.gen37_production_shaped_fixture()
        closure = deepcopy(strict["child_closures"][0])
        closure["unexpected"] = "must refuse"
        with self.assertRaisesRegex(
            LinearProjectionError, "invalid_projection_child_closure",
        ):
            build_projection_event(
                workstream_id="GEN-37", kind="child_closure",
                key=closure["child_identifier"], value=closure,
                plan_revision=PLAN, expected_revision=0,
                created_at="2026-08-29T20:00:00Z", authority=AUTHORITY,
            )

        tampered = deepcopy(strict)
        tampered["child_closures"][0]["child_readback_sha256"] = "0" * 64
        with self.assertRaisesRegex(
            ResumeError, "projection_current_view_mismatch:child_closures",
        ):
            compact_context(
                tampered, "GEN-37", max_bytes=16 * 1024, max_items=100,
                require_projection_authority=True,
            )

    def test_compact_provenance_does_not_invent_ordered_supersession(self):
        older = {
            "agent": "zeta", "machine": "M5", "session_id": "older",
            "worktree": {"state": "safe", "head": HEAD},
        }
        later = {
            "agent": "alpha", "machine": "M3", "session_id": "later",
            "worktree": {
                "state": "dirty", "path": "/dirty", "head": "b" * 40,
            },
        }
        items = sorted(
            [older, later],
            key=lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")),
        )
        self.assertEqual(items[-1]["session_id"], "older")
        projection_events = [
            {"kind": "provenance", "key": "older", "event_id": "event-old",
             "value": older},
            {"kind": "provenance", "key": "later", "event_id": "event-new",
             "value": later},
        ]
        compact = resume_module._compact_provenance(
            items, projection_events,
        )
        self.assertEqual(compact["worktree_authority_count"], 2)
        self.assertTrue(compact["worktree_authority_ambiguous"])
        self.assertIsNone(compact["latest"])
        self.assertIsNone(compact["latest_projection_head"])

        sole_dirty = resume_module._compact_provenance(
            [later], [projection_events[1]],
        )
        self.assertEqual(sole_dirty["worktree_authority_count"], 1)
        self.assertEqual(sole_dirty["latest"]["worktree"], later["worktree"])
        self.assertEqual(
            sole_dirty["latest_projection_head"]["key"], "later",
        )

    def test_multi_terminal_repair_is_ordered_full_and_idempotent(self):
        client, adapter, source, graph, _children, stale_manifest = (
            self.multi_terminal_repair_fixture()
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
        expected = {
            repair["child_identifier"]: repair[
                "expected_child_readback_sha256"
            ]
            for repair in stale_manifest["terminal_child_repairs"]
        }
        before_revision = adapter.state().revision
        result = reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-27T19:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected[child_id] for child_id in child_ids
            },
            legacy_unresolved_relation_heads=unresolved,
        )
        appended = adapter.state().events[before_revision:]
        self.assertEqual(
            [(event["kind"], event["key"]) for event in appended],
            [("child_closure", "GEN-70"), ("child_closure", "GEN-72")],
        )
        strict = add_material_history(
            graph, client.comments, "GEN-37",
            authenticated_route=AUTHORITY, authenticated_source=source,
        )
        context = compact_context(
            strict, "GEN-37", require_projection_authority=True,
        )
        self.assertEqual(context["resume_authority"], "full")
        self.assertEqual(
            [closure["child_identifier"] for closure in context["child_closures"]],
            ["GEN-70", "GEN-72"],
        )
        self.assertTrue(result["readback_verified"])

        replay = prepare_terminal_child_repairs(
            stale_manifest, graph, adapter.state(),
        )
        replay_preview, replay_unresolved = (
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", replay, adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        )
        comments_before = len(client.comments)
        replay_result = reconcile_required_projection(
            adapter, replay_preview, replay, remote_head=HEAD,
            created_at="2026-08-27T20:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected[child_id] for child_id in child_ids
            },
            legacy_unresolved_relation_heads=replay_unresolved,
        )
        self.assertEqual(replay_result["writes"], [])
        self.assertEqual(len(client.comments), comments_before)
        self.assertFalse(any(
            "issueCreate" in query or "issueUpdate" in query
            for query, _variables in client.calls
        ))

    def test_terminal_repair_preserves_explicitly_unassigned_child(self):
        client, adapter, source, graph, _children, manifest = (
            self.multi_terminal_repair_fixture(unassigned_child="GEN-70")
        )
        prepared = prepare_terminal_child_repairs(
            manifest, graph, adapter.state(),
        )
        unassigned_closure = deepcopy(next(
            item["value"] for item in prepared["projection"]
            if (item["kind"], item["key"])
            == ("child_closure", "GEN-70")
        ))
        self.assertEqual(unassigned_closure["schema_version"], 2)
        unassigned_closure["schema_version"] = 1
        with self.assertRaisesRegex(
            LinearProjectionError, "invalid_projection_child_closure",
        ):
            build_projection_event(
                workstream_id="GEN-37", kind="child_closure", key="GEN-70",
                value=unassigned_closure, plan_revision=PLAN,
                expected_revision=adapter.state().revision,
                created_at="2026-08-27T18:30:00Z", authority=AUTHORITY,
            )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = {
            repair["child_identifier"]:
            repair["expected_child_readback_sha256"]
            for repair in manifest["terminal_child_repairs"]
        }
        reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-27T19:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected[child_id] for child_id in child_ids
            },
            legacy_unresolved_relation_heads=unresolved,
        )
        context = compact_context(
            add_material_history(
                graph, client.comments, "GEN-37",
                authenticated_route=AUTHORITY, authenticated_source=source,
            ),
            "GEN-37", require_projection_authority=True,
        )
        gen70 = next(
            closure for closure in context["child_closures"]
            if closure["child_identifier"] == "GEN-70"
        )
        self.assertIsNone(gen70["assignee_id"])
        self.assertEqual(gen70["state_name"], "Done")
        self.assertEqual(gen70["state_type"], "completed")
        self.assertEqual(context["resume_authority"], "full")

    def test_terminal_readback_distinguishes_unassigned_from_missing_field(self):
        _client, _adapter, _source, _graph, children, _manifest = (
            self.multi_terminal_repair_fixture(unassigned_child="GEN-70")
        )
        child = next(
            item for item in children if item["identifier"] == "GEN-70"
        )
        self.assertIsNone(terminal_child_readback(child)["assignee_id"])
        child.pop("assignee")
        with self.assertRaisesRegex(
            ChildClosureError, "terminal_child_readback_missing:assignee",
        ):
            terminal_child_readback(child)

    def test_terminal_evidence_seed_is_add_only_partial_and_idempotent(self):
        client, adapter, source, graph, _children, stale_manifest = (
            self.multi_terminal_repair_fixture(evidence_active=False)
        )
        prepared = prepare_terminal_child_evidence_seeds(
            stale_manifest, graph, adapter.state(),
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in stale_manifest["terminal_child_evidence_seeds"]
        }
        before_revision = adapter.state().revision
        result = reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-27T19:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected[child_id] for child_id in child_ids
            },
            legacy_unresolved_relation_heads=unresolved,
        )
        self.assertEqual(
            [(event["kind"], event["key"])
             for event in adapter.state().events[before_revision:]],
            [
                ("evidence_contract", "gen-70-terminal"),
                ("evidence_contract", "gen-72-terminal"),
            ],
        )
        self.assertFalse(result["resume_authority_verified"])
        with self.assertRaisesRegex(
            ResumeError, "completed_owned_child_closure_missing",
        ):
            compact_context(
                add_material_history(
                    graph, client.comments, "GEN-37",
                    authenticated_route=AUTHORITY,
                    authenticated_source=source,
                ),
                "GEN-37", require_projection_authority=True,
            )

        replay = prepare_terminal_child_evidence_seeds(
            stale_manifest, graph, adapter.state(),
        )
        replay_preview, replay_unresolved = (
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", replay, adapter,
                authenticated_route=AUTHORITY,
                authenticated_source=source, remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        )
        comments_before = len(client.comments)
        replay_result = reconcile_required_projection(
            adapter, replay_preview, replay, remote_head=HEAD,
            created_at="2026-08-27T20:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected[child_id] for child_id in child_ids
            },
            legacy_unresolved_relation_heads=replay_unresolved,
        )
        self.assertEqual(replay_result["writes"], [])
        self.assertEqual(len(client.comments), comments_before)

    def test_new_plan_generation_bootstraps_terminal_evidence_then_repairs(self):
        (_old_client, _old_adapter, source, graph, children,
         existing_generation_manifest) = self.multi_terminal_repair_fixture(
            evidence_active=False,
        )
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        manifest = {
            **reviewed_manifest(
                adapter, deepcopy(existing_generation_manifest["projection"]),
            ),
            "terminal_child_evidence_seeds": deepcopy(
                existing_generation_manifest["terminal_child_evidence_seeds"]
            ),
        }
        self.assertEqual(manifest["expected_projection_revision"], 0)
        self.assertEqual(manifest["expected_active_heads"], [])

        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=HEAD,
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected_readbacks = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        first = reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-29T22:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected_readbacks[child_id]
                for child_id in child_ids
            },
            legacy_unresolved_relation_heads=unresolved,
        )
        self.assertEqual(
            [(event["kind"], event["key"]) for event in adapter.state().events],
            [
                ("evidence_contract", "gen-70-terminal"),
                ("evidence_contract", "gen-72-terminal"),
                ("source", "root"),
                ("provenance", "session"),
                ("disposition", "root"),
                ("scope", "root"),
            ],
        )
        self.assertFalse(first["resume_authority_verified"])

        replay = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=HEAD,
        )
        replay_preview, replay_unresolved = (
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", replay, adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        )
        comments_before = len(client.comments)
        replay_result = reconcile_required_projection(
            adapter, replay_preview, replay, remote_head=HEAD,
            created_at="2026-08-29T22:01:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected_readbacks[child_id]
                for child_id in child_ids
            },
            legacy_unresolved_relation_heads=replay_unresolved,
        )
        self.assertEqual(replay_result["writes"], [])
        self.assertEqual(len(client.comments), comments_before)

        active = workstream_projection._active_heads(adapter.state())
        repairs = []
        for child in children:
            child_id = child["identifier"]
            evidence_event = next(
                event for (kind, _key), event in active.items()
                if kind == "evidence_contract"
                and event["value"]["owning_child"] == child_id
            )
            repairs.append({
                "child_identifier": child_id,
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
            })
        repair_manifest = {
            **reviewed_manifest(
                adapter, deepcopy(existing_generation_manifest["projection"]),
            ),
            "terminal_child_repairs": repairs,
        }
        repair_prepared = prepare_terminal_child_repairs(
            repair_manifest, graph, adapter.state(),
        )
        repair_preview, repair_unresolved = (
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", repair_prepared, adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        )
        second = reconcile_required_projection(
            adapter, repair_preview, repair_prepared, remote_head=HEAD,
            created_at="2026-08-29T22:02:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected_readbacks[child_id]
                for child_id in child_ids
            },
            legacy_unresolved_relation_heads=repair_unresolved,
        )
        self.assertTrue(second["resume_authority_verified"])
        context = compact_context(
            add_material_history(
                graph, client.comments, "GEN-37",
                authenticated_route=AUTHORITY, authenticated_source=source,
            ),
            "GEN-37", require_projection_authority=True,
        )
        self.assertEqual(context["resume_authority"], "full")

    def test_new_plan_generation_terminal_seed_bootstrap_refuses_widening(self):
        def fixture():
            (_old_client, _old_adapter, source, graph, _children,
             existing_generation_manifest) = self.multi_terminal_repair_fixture(
                evidence_active=False,
            )
            client = FakeProjectionClient()
            adapter = LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=PLAN, **AUTHORITY,
            )
            manifest = {
                **reviewed_manifest(
                    adapter,
                    deepcopy(existing_generation_manifest["projection"]),
                ),
                "terminal_child_evidence_seeds": deepcopy(
                    existing_generation_manifest[
                        "terminal_child_evidence_seeds"
                    ]
                ),
            }
            return client, adapter, source, graph, manifest

        mutations = {
            "nonempty_review_contract": (
                lambda manifest: manifest.__setitem__(
                    "expected_projection_revision", 1,
                ),
                "terminal_child_evidence_seed_scope_missing",
            ),
            "incomplete_seed_set": (
                lambda manifest: manifest[
                    "terminal_child_evidence_seeds"
                ].pop(),
                "terminal_child_evidence_seed_bootstrap_unrelated_change",
            ),
            "relation": (
                lambda manifest: manifest["projection"].append({
                    "kind": "relation", "key": "related:GEN-99",
                    "value": {"type": "related", "target": {
                        "workspace_id": "workspace",
                        "issue_id": TARGET_UUID,
                        "identifier": "GEN-99",
                    }},
                }),
                "terminal_child_evidence_seed_bootstrap_unrelated_change",
            ),
            "route": (
                lambda manifest: next(
                    item for item in manifest["projection"]
                    if item["kind"] == "scope"
                )["value"]["linear"].__setitem__("project_id", "wrong"),
                "terminal_child_evidence_seed_route_mismatch:project_id",
            ),
            "evidence_head": (
                lambda manifest: next(
                    item for item in manifest["projection"]
                    if item["kind"] == "evidence_contract"
                )["value"].__setitem__("exact_head", "b" * 40),
                "terminal_child_evidence_seed_contract_invalid",
            ),
        }
        for name, (mutate, message) in mutations.items():
            with self.subTest(name=name):
                client, adapter, _source, graph, manifest = fixture()
                mutate(manifest)
                with self.assertRaisesRegex(LinearProjectionError, message):
                    prepare_terminal_child_evidence_seeds(
                        manifest, graph, adapter.state(), remote_head=HEAD,
                    )
                self.assertEqual(client.comments, [])

    def test_new_plan_generation_carries_mixed_historical_heads_then_repairs(self):
        client, adapter, source, graph, children, manifest, binding = (
            self.mixed_head_plan_generation_fixture()
        )
        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=HEAD,
            comments=client.comments,
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        readbacks = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        first = reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-29T22:30:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: readbacks[child_id] for child_id in child_ids
            },
            projection_input_fence=lambda: binding["input_frontier_sha256"],
            checkpoint_fence=lambda: None,
            projection_comments=client.comments,
            projection_input_snapshot=graph,
            legacy_unresolved_relation_heads=unresolved,
        )
        self.assertFalse(first["resume_authority_verified"])
        self.assertEqual(
            {
                event["value"]["exact_head"]
                for event in adapter.state().events
                if event["kind"] == "evidence_contract"
            },
            {"1" * 40, "2" * 40},
        )

        replay = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=HEAD,
            comments=client.comments,
        )
        replay_preview, replay_unresolved = (
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", replay, adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        )
        before_replay = len(client.comments)
        replay_result = reconcile_required_projection(
            adapter, replay_preview, replay, remote_head=HEAD,
            created_at="2026-08-29T22:31:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: readbacks[child_id] for child_id in child_ids
            },
            projection_input_fence=lambda: binding["input_frontier_sha256"],
            checkpoint_fence=lambda: None,
            projection_comments=client.comments,
            projection_input_snapshot=graph,
            legacy_unresolved_relation_heads=replay_unresolved,
        )
        self.assertEqual(replay_result["writes"], [])
        self.assertEqual(len(client.comments), before_replay)

        active = workstream_projection._active_heads(adapter.state())
        repairs = []
        for child in children:
            event = next(
                event for (kind, _key), event in active.items()
                if kind == "evidence_contract"
                and event["value"]["owning_child"] == child["identifier"]
            )
            repairs.append({
                "child_identifier": child["identifier"],
                "child_issue_id": child["id"],
                "expected_child_readback_sha256": readbacks[child["identifier"]],
                "expected_assignee_id": child["assignee"]["id"],
                "approved_evidence_heads": [{
                    "key": event["key"], "event_id": event["event_id"],
                    "value_sha256": canonical_digest(event["value"]),
                }],
            })
        repair_projection = [
            {"kind": kind, "key": key, "value": deepcopy(event["value"])}
            for (kind, key), event in sorted(active.items())
            if kind != "disposition"
        ]
        repair_manifest = {
            **reviewed_manifest(adapter, repair_projection),
            "terminal_child_repairs": repairs,
        }
        repair_prepared = prepare_terminal_child_repairs(
            repair_manifest, graph, adapter.state(),
        )
        repair_preview, repair_unresolved = (
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", repair_prepared, adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        )
        second = reconcile_required_projection(
            adapter, repair_preview, repair_prepared, remote_head=HEAD,
            created_at="2026-08-29T22:32:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: readbacks[child_id] for child_id in child_ids
            },
            projection_input_fence=lambda: binding["input_frontier_sha256"],
            checkpoint_fence=lambda: None,
            legacy_unresolved_relation_heads=repair_unresolved,
        )
        self.assertTrue(second["resume_authority_verified"])
        context = compact_context(
            add_material_history(
                graph, client.comments, "GEN-37",
                authenticated_route=AUTHORITY, authenticated_source=source,
            ),
            "GEN-37", require_projection_authority=True,
        )
        self.assertEqual(context["resume_authority"], "full")
        self.assertEqual(len(context["child_closures"]), 5)

    def test_generation_activation_post_read_accepts_exact_predecessor_control(self):
        fixture = self.mixed_head_repair_reconcile_fixture(
            activation_ready=True,
        )
        client, target, source, graph = fixture[:4]
        self.run_mixed_head_repair_reconcile(fixture)
        predecessor_plan = "b" * 64
        activation_checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="activate-target",
            root_revision=1, plan_revision=PLAN,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "activate-target", "machine": "M5",
                "worktree": {
                    "state": "safe", "path": "/tmp/target",
                    "branch": "target", "head": HEAD,
                },
            },
            exact_head=HEAD, evidence=[], blocker=None,
            next_action="Continue the activated target generation.",
        )

        def candidate_loader(plan_revision):
            self.assertEqual(plan_revision, PLAN)
            state = target.state()
            candidate_graph = deepcopy(graph)
            selected = select_plan_generation(
                client.comments, workstream_id="GEN-37",
                description_plan_revision=predecessor_plan,
                authenticated_route=AUTHORITY,
            )
            candidate_comments = client.comments
            if selected["plan_revision"] == plan_revision:
                candidate_graph["root"].update({
                    "generation_transition_tip_event_id": selected[
                        "transition_tip_event_id"
                    ],
                    "generation_activation_epoch": selected[
                        "activation_epoch"
                    ],
                    "generation_authority_origin": selected[
                        "authority_origin"
                    ],
                    "description_plan_revision": predecessor_plan,
                })
            else:
                candidate_comments = [*client.comments, {
                    "id": "00000000-0000-4000-8000-000000000000",
                    "body": encode_checkpoint_comment(activation_checkpoint),
                }]
            material = reduce_event_comments(
                candidate_comments, workstream_id="GEN-37",
            )
            checkpoint_ids = sorted(
                checkpoint["event_id"]
                for checkpoint in reduce_checkpoint_comments(
                    candidate_comments, workstream_id="GEN-37",
                ).checkpoints
                if checkpoint["plan_revision"] == plan_revision
            )
            checkpoint_ids = sorted(set([
                *checkpoint_ids, activation_checkpoint["event_id"],
            ]))
            context = compact_context(
                add_material_history(
                    candidate_graph, candidate_comments, "GEN-37",
                    authenticated_route=AUTHORITY,
                    authenticated_source=source,
                ),
                "GEN-37", require_projection_authority=True,
            )
            return {
                "resume_authority": context["resume_authority"],
                "plan_revision": plan_revision,
                "authenticated_route": AUTHORITY,
                "source": source,
                "material_revision": material.revision,
                "checkpoint_event_ids": checkpoint_ids,
                "projection_revision": state.revision,
                "graph_frontier_sha256": generation_digest({
                    "root": graph["root"], "children": graph["children"],
                    "decisions": graph.get("decisions", []),
                }),
                "snapshot_sha256": generation_digest({
                    "resume_authority": context["resume_authority"],
                    "projection_event_ids": [
                        event["event_id"] for event in state.events
                    ],
                }),
                "quarantined_legacy_writes": generation_quarantine_metadata(
                    client.comments, workstream_id="GEN-37",
                ),
            }

        predecessor = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=predecessor_plan, **AUTHORITY,
        ).state()
        checkpoint_ids = sorted(
            checkpoint["event_id"]
            for checkpoint in reduce_checkpoint_comments(
                client.comments, workstream_id="GEN-37",
            ).checkpoints
            if checkpoint["plan_revision"] == predecessor_plan
        )
        retirement = build_retirement_proof(
            predecessor_plan_revision=predecessor_plan,
            retired_at="2026-08-31T17:00:00Z", retired_writer_epoch=0,
            provenance_event_ids=sorted(
                event["event_id"] for event in predecessor.events
                if event["kind"] == "provenance"
            ),
            checkpoint_event_ids=checkpoint_ids,
        )
        disposition = next(
            event for event in reversed(target.state().events)
            if event["kind"] == "disposition" and event["key"] == "root"
        )
        target.append(build_projection_event(
            workstream_id="GEN-37", kind="disposition", key="root",
            value={
                "disposition": "attach", "remote_head": HEAD,
                "recovered_from_checkpoint": activation_checkpoint["event_id"],
            },
            plan_revision=PLAN, expected_revision=target.state().revision,
            created_at="2026-08-31T16:15:00Z",
            supersedes_event_id=disposition["event_id"], authority=AUTHORITY,
        ))
        while target.state().revision < 17:
            revision = target.state().revision
            target.append(build_projection_event(
                workstream_id="GEN-37", kind="provenance",
                key=f"target-production-padding-{revision}",
                value={
                    "agent": "codex", "machine": "M5",
                    "session_id": f"target-production-padding-{revision}",
                    "worktree": {"state": "safe", "head": HEAD},
                },
                plan_revision=PLAN, expected_revision=revision,
                created_at=f"2026-08-31T16:{revision:02d}:00Z",
                authority=AUTHORITY,
            ))
        before_target_revision = target.state().revision
        before_predecessor_revision = predecessor.revision
        before_writes = len(client.comments)
        self.assertEqual(before_target_revision, 17)
        self.assertEqual(before_predecessor_revision, 85)
        transport = GenerationTransport(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            authority=AUTHORITY, candidate_loader=candidate_loader,
            legacy_description_plan_revision=predecessor_plan,
        )
        transport._capability_checked = True
        result = transport.activate(
            target_plan_revision=PLAN,
            created_at="2026-08-31T17:00:00Z", retirement=retirement,
            activation_checkpoint=activation_checkpoint, remote_head=HEAD,
        )

        self.assertEqual(len(client.comments), before_writes + 3)
        self.assertEqual(target.state().revision, 18)
        self.assertEqual(
            LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=predecessor_plan, **AUTHORITY,
            ).state().revision,
            86,
        )
        self.assertEqual(result["activated_plan_revision"], PLAN)
        self.assertEqual(candidate_loader(PLAN)["resume_authority"], "full")

        selected = select_plan_generation(
            client.comments, workstream_id="GEN-37",
            description_plan_revision=predecessor_plan,
            authenticated_route=AUTHORITY,
        )
        post_graph = deepcopy(graph)
        post_graph["root"].update({
            "generation_transition_tip_event_id": selected[
                "transition_tip_event_id"
            ],
            "generation_activation_epoch": selected["activation_epoch"],
            "generation_authority_origin": selected["authority_origin"],
            "description_plan_revision": predecessor_plan,
        })
        post_snapshot = add_material_history(
            post_graph, client.comments, "GEN-37",
            authenticated_route=AUTHORITY, authenticated_source=source,
        )
        self.assertEqual(compact_context(
            post_snapshot, "GEN-37", require_projection_authority=True,
        )["resume_authority"], "full")

        def set_value(path, replacement):
            def mutate(event):
                target_value = event
                for field in path[:-1]:
                    target_value = target_value[field]
                old = target_value[path[-1]]
                target_value[path[-1]] = (
                    replacement(old) if callable(replacement) else replacement
                )
            return mutate

        def change_checkpoints(side):
            def mutate(event):
                frontier = event["value"][side]
                frontier["checkpoint_event_ids"] = sorted([
                    *frontier["checkpoint_event_ids"], "checkpoint-other",
                ])
                frontier["checkpoint_events_sha256"] = generation_digest(
                    frontier["checkpoint_event_ids"]
                )
            return mutate

        def change_retirement(event):
            retirement = event["value"]["retirement"]
            retirement["retired_at"] = "changed"
            retirement["declaration_sha256"] = generation_digest({
                key: value for key, value in retirement.items()
                if key != "declaration_sha256"
            })

        mutations = {
            "terminal_event": set_value(("created_at",), "changed"),
            "from_projection_revision": set_value(
                ("value", "from", "projection_revision"), lambda value: value + 1,
            ),
            "from_projection_digest": set_value(
                ("value", "from", "projection_events_sha256"), "0" * 64,
            ),
            "from_projection_frontier": set_value(
                ("value", "from", "projection_frontier_event_id"),
                "wsp_" + "0" * 32,
            ),
            "from_material": set_value(
                ("value", "from", "material_revision"), lambda value: value + 1,
            ),
            "from_checkpoints": change_checkpoints("from"),
            "from_source_event": set_value(
                ("value", "from", "source_event_id"), "wsp_" + "0" * 32,
            ),
            "from_source_identity": set_value(
                ("value", "from", "source_identity"), "https://wrong.test/plan",
            ),
            "from_source_sha": set_value(
                ("value", "from", "source_sha256"), "0" * 64,
            ),
            "to_projection_revision": set_value(
                ("value", "to", "projection_revision"), lambda value: value + 1,
            ),
            "to_projection_digest": set_value(
                ("value", "to", "projection_events_sha256"), "0" * 64,
            ),
            "to_projection_frontier": set_value(
                ("value", "to", "projection_frontier_event_id"),
                "wsp_" + "0" * 32,
            ),
            "to_material": set_value(
                ("value", "to", "material_revision"), lambda value: value + 1,
            ),
            "to_checkpoints": change_checkpoints("to"),
            "to_source_event": set_value(
                ("value", "to", "source_event_id"), "wsp_" + "0" * 32,
            ),
            "to_source_identity": set_value(
                ("value", "to", "source_identity"), "https://wrong.test/plan",
            ),
            "to_source_sha": set_value(
                ("value", "to", "source_sha256"), "0" * 64,
            ),
            "source": set_value(
                ("value", "source", "identity"), "https://wrong.test/plan",
            ),
            "reservation_id": set_value(
                ("value", "reservation_id"), "wsgr_" + "0" * 32,
            ),
            "reservation_sha": set_value(
                ("value", "reservation_sha256"), "0" * 64,
            ),
            "graph_frontier": set_value(
                ("value", "graph_frontier_sha256"), "0" * 64,
            ),
            "candidate_resume": set_value(
                ("value", "candidate_resume_sha256"), "0" * 64,
            ),
            "candidate_seal_id": set_value(
                ("value", "candidate_seal_event_id"), "wsp_" + "0" * 32,
            ),
            "candidate_seal_sha": set_value(
                ("value", "candidate_seal_sha256"), "0" * 64,
            ),
            "activation_epoch": set_value(
                ("value", "activation_epoch"), lambda value: value + 1,
            ),
            "retirement": change_retirement,
            "previous_control": set_value(
                ("value", "previous_control_event_id"), "wsp_" + "0" * 32,
            ),
            "activation_checkpoint": set_value(
                ("value", "activation_checkpoint", "next_action"), "changed",
            ),
            "activation_checkpoint_sha": set_value(
                ("value", "activation_checkpoint_sha256"), "0" * 64,
            ),
        }
        for name, mutate in mutations.items():
            with self.subTest(authoritative_field=name):
                changed = deepcopy(post_snapshot)
                transition = next(
                    event for event in changed["projection_history"]
                    if event["event_id"] == selected["transition_tip_event_id"]
                )
                mutate(transition)
                transition["event_id"] = projection_module._event_id(transition)
                with self.assertRaises(ResumeError):
                    compact_context(
                        changed, "GEN-37", require_projection_authority=True,
                    )

        additional = deepcopy(post_snapshot)
        additional["projection_history"].append(build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="late-suffix",
            value={
                "agent": "codex", "machine": "M5", "session_id": "late",
                "worktree": {"state": "safe", "head": HEAD},
            },
            plan_revision=predecessor_plan, expected_revision=86,
            created_at="2026-08-31T18:00:00Z", authority=AUTHORITY,
        ))
        additional["projection_recovery"]["stale_plan_count"] += 1
        with self.assertRaisesRegex(
            ResumeError, "carried_evidence_authority_invalid",
        ):
            compact_context(
                additional, "GEN-37", require_projection_authority=True,
            )

    def test_mixed_head_predecessor_binding_refuses_omission_and_drift(self):
        def omit_child(manifest):
            manifest["terminal_child_evidence_seeds"].pop()
            manifest["projection"] = [
                item for item in manifest["projection"]
                if item.get("key") != "gen-85-terminal"
            ]

        def mutate_contract(manifest):
            contract = next(
                item["value"] for item in manifest["projection"]
                if item.get("key") == "gen-38-terminal"
            )
            contract["layers"]["logic"]["receipts"][0]["proof"] = "mutated"

        mutations = {
            "omission": omit_child,
            "contract": mutate_contract,
            "projection": lambda manifest: manifest[
                "terminal_child_evidence_seed_predecessor"
            ].__setitem__("projection_events_sha256", "f" * 64),
            "history": lambda manifest: manifest[
                "terminal_child_evidence_seed_predecessor"
            ].__setitem__("projection_history_sha256", "f" * 64),
            "material": lambda manifest: manifest[
                "terminal_child_evidence_seed_predecessor"
            ].__setitem__("material_events_sha256", "f" * 64),
            "checkpoint": lambda manifest: manifest[
                "terminal_child_evidence_seed_predecessor"
            ].__setitem__("checkpoint_event_id", "wrong-checkpoint"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                client, adapter, _source, graph, _children, manifest, _binding = (
                    self.mixed_head_plan_generation_fixture()
                )
                before = len(client.comments)
                mutate(manifest)
                with self.assertRaises(LinearProjectionError):
                    prepare_terminal_child_evidence_seeds(
                        manifest, graph, adapter.state(), remote_head=HEAD,
                        comments=client.comments,
                    )
                self.assertEqual(len(client.comments), before)

    def test_mixed_head_bootstrap_refuses_bare_seeds_with_stale_history(self):
        client, adapter, _source, graph, _children, manifest, _binding = (
            self.mixed_head_plan_generation_fixture()
        )
        manifest.pop("terminal_child_evidence_seed_predecessor")
        before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_seed_predecessor_binding_required",
        ):
            prepare_terminal_child_evidence_seeds(
                manifest, graph, adapter.state(), remote_head=HEAD,
                comments=client.comments,
            )
        self.assertEqual(len(client.comments), before)

    def test_mixed_head_seed_fences_late_predecessor_append_at_every_prefix(self):
        baseline = self.mixed_head_plan_generation_fixture()
        baseline_result = self.run_mixed_head_seed_reconcile(
            baseline[0], baseline[1], baseline[2], baseline[3],
            baseline[5], baseline[6],
        )
        append_count = len(baseline_result["writes"])
        self.assertGreater(append_count, 1)
        for prefix in range(1, append_count + 1):
            with self.subTest(prefix=prefix):
                client, adapter, source, graph, _children, manifest, binding = (
                    self.mixed_head_plan_generation_fixture()
                )
                self.install_late_predecessor_projection_append(
                    client, adapter, after_append=prefix, suffix=prefix,
                )
                with self.assertRaisesRegex(
                    LinearProjectionError,
                    "terminal_seed_predecessor_history_changed_reload_required",
                ):
                    self.run_mixed_head_seed_reconcile(
                        client, adapter, source, graph, manifest, binding,
                    )

    def test_mixed_head_repair_fences_late_predecessor_append_at_every_prefix(self):
        baseline = self.mixed_head_repair_reconcile_fixture()
        baseline_result = self.run_mixed_head_repair_reconcile(baseline)
        append_count = len(baseline_result["writes"])
        self.assertEqual(append_count, 5)
        for prefix in range(1, append_count + 1):
            with self.subTest(prefix=prefix):
                fixture = self.mixed_head_repair_reconcile_fixture()
                client, adapter = fixture[:2]
                self.install_late_predecessor_projection_append(
                    client, adapter, after_append=prefix,
                    suffix=20 + prefix,
                )
                with self.assertRaisesRegex(
                    LinearProjectionError,
                    "terminal_seed_predecessor_history_changed_reload_required",
                ):
                    self.run_mixed_head_repair_reconcile(fixture)

    def test_mixed_head_predecessor_frontier_drift_refuses_before_write(self):
        client, adapter, source, graph, _children, manifest, _binding = (
            self.mixed_head_plan_generation_fixture()
        )
        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=HEAD,
            comments=client.comments,
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        readbacks = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        before_revision = adapter.state().revision
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_input_frontier_changed",
        ):
            reconcile_required_projection(
                adapter, preview, prepared, remote_head=HEAD,
                created_at="2026-08-29T22:40:00Z",
                authenticated_source=source,
                relation_target_resolver=self.relation_target_resolver,
                terminal_child_fence=lambda child_ids: {
                    child_id: readbacks[child_id] for child_id in child_ids
                },
                projection_input_fence=lambda: "f" * 64,
                checkpoint_fence=lambda: None,
                projection_comments=client.comments,
                projection_input_snapshot=graph,
                legacy_unresolved_relation_heads=unresolved,
            )
        self.assertEqual(adapter.state().revision, before_revision)

    def test_mixed_head_predecessor_seed_recovers_from_crash_prefix(self):
        client, adapter, source, graph, _children, manifest, binding = (
            self.mixed_head_plan_generation_fixture()
        )
        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=HEAD,
            comments=client.comments,
        )
        evidence_items = [
            item for item in prepared["projection"]
            if item["kind"] == "evidence_contract"
        ]
        for item in evidence_items[:2]:
            adapter.append(build_projection_event(
                workstream_id="GEN-37", kind=item["kind"], key=item["key"],
                value=item["value"], plan_revision=PLAN,
                expected_revision=adapter.state().revision,
                created_at="2026-08-29T22:45:00Z", authority=AUTHORITY,
            ))
        resumed = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=HEAD,
            comments=client.comments,
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", resumed, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        readbacks = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        result = reconcile_required_projection(
            adapter, preview, resumed, remote_head=HEAD,
            created_at="2026-08-29T22:46:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: readbacks[child_id] for child_id in child_ids
            },
            projection_input_fence=lambda: binding["input_frontier_sha256"],
            checkpoint_fence=lambda: None,
            projection_comments=client.comments,
            projection_input_snapshot=graph,
            legacy_unresolved_relation_heads=unresolved,
        )
        self.assertFalse(result["resume_authority_verified"])
        self.assertEqual(
            len([
                event for event in adapter.state().events
                if event["kind"] == "evidence_contract"
            ]),
            5,
        )

    def terminal_seed_head_transition_fixture(self):
        client, adapter, source, graph, children, manifest = (
            self.multi_terminal_repair_fixture(evidence_active=False)
        )
        new_head = "b" * 40
        desired_scope = next(
            item["value"] for item in manifest["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        primary = next(
            repository for repository in desired_scope["repositories"]
            if repository_key(repository) == desired_scope["primary_repository"]
        )
        primary["exact_head"] = new_head
        for item in manifest["projection"]:
            if item["kind"] == "evidence_contract":
                item["value"]["exact_head"] = new_head
                for layer in item["value"]["layers"].values():
                    for receipt in layer.get("receipts", []):
                        receipt["exact_head"] = new_head
        frontier = workstream_projection.projection_input_frontier_sha256(
            graph, client.comments,
        )
        from_scope_event = workstream_projection._active_heads(
            adapter.state()
        )[("scope", "root")]
        from_disposition_event = workstream_projection._active_heads(
            adapter.state()
        )[("disposition", "root")]
        manifest["terminal_child_evidence_seed_head_transition"] = {
            "repository_key": desired_scope["primary_repository"],
            "from_exact_head": HEAD, "to_exact_head": new_head,
            "from_scope_event_id": from_scope_event["event_id"],
            "from_scope_value_sha256": canonical_digest(
                from_scope_event["value"]
            ),
            "from_disposition_event_id": from_disposition_event["event_id"],
            "from_disposition_value_sha256": canonical_digest(
                from_disposition_event["value"]
            ),
            "disposition": {
                "disposition": "create_successor", "remote_head": new_head,
                "recovered_from_checkpoint": None,
            },
            "input_frontier_sha256": frontier,
        }
        return client, adapter, source, graph, children, manifest, new_head

    def test_terminal_seed_can_advance_primary_head_without_rewriting_history(self):
        client, adapter, source, graph, _children, manifest, new_head = (
            self.terminal_seed_head_transition_fixture()
        )
        self.assertNotIn(
            "created_at",
            manifest["terminal_child_evidence_seed_head_transition"],
        )
        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=new_head,
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=new_head,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        before_revision = adapter.state().revision
        result = reconcile_required_projection(
            adapter, preview, prepared, remote_head=new_head,
            created_at="2026-08-29T21:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected[child_id] for child_id in child_ids
            },
            projection_input_fence=lambda: prepared[
                "terminal_child_evidence_seed_head_transition"
            ]["input_frontier_sha256"],
            legacy_unresolved_relation_heads=unresolved,
        )
        appended = adapter.state().events[before_revision:]
        self.assertEqual(
            [(event["kind"], event["key"]) for event in appended],
            [
                ("evidence_contract", "gen-70-terminal"),
                ("evidence_contract", "gen-72-terminal"),
                ("disposition", "root"),
                ("scope", "root"),
            ],
        )
        current_scope = next(
            event["value"] for event in reversed(adapter.state().events)
            if (event["kind"], event["key"]) == ("scope", "root")
        )
        repository = next(
            item for item in current_scope["repositories"]
            if repository_key(item) == current_scope["primary_repository"]
        )
        self.assertEqual(repository["exact_head"], new_head)
        self.assertFalse(result["resume_authority_verified"])

        replay = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=new_head,
        )
        replay_preview, replay_unresolved = (
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", replay, adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                remote_head=new_head,
                relation_target_resolver=self.relation_target_resolver,
            )
        )
        comments_before = len(client.comments)
        replay_result = reconcile_required_projection(
            adapter, replay_preview, replay, remote_head=new_head,
            created_at="2026-08-29T21:01:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected[child_id] for child_id in child_ids
            },
            projection_input_fence=lambda: replay[
                "terminal_child_evidence_seed_head_transition"
            ]["input_frontier_sha256"],
            legacy_unresolved_relation_heads=replay_unresolved,
        )
        self.assertEqual(replay_result["writes"], [])
        self.assertEqual(len(client.comments), comments_before)

    def test_predecessor_seed_rebinds_only_primary_then_repairs_children(self):
        (
            client, adapter, source, graph, children, manifest, binding,
        ) = self.mixed_head_plan_generation_fixture()
        desired_scope = next(
            item["value"] for item in manifest["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        secondary = deepcopy(desired_scope["repositories"][0])
        secondary.update({
            "provider_repository_id": "shipyard-id",
            "slug": "github.com/acme/shipyard",
            "exact_head": "d513461fed3571a18f748aa9dd939d5c431ee957",
            "identity_resolution": {
                "provider_repository_id": "shipyard-id",
                "resolved_slug": "github.com/acme/shipyard",
                "observed_at": "2026-09-01T09:00:00Z",
                "evidence": [{
                    "kind": "authenticated_provider_readback",
                    "authenticated": True,
                    "provider_repository_id": "shipyard-id",
                    "resolved_slug": "github.com/acme/shipyard",
                }],
            },
        })
        desired_scope["repositories"].append(secondary)
        desired_contracts = {
            item["key"]: item["value"] for item in manifest["projection"]
            if item["kind"] == "evidence_contract"
        }
        binding, _authorities = (
            terminal_child_evidence_seed_predecessor_contract(
                graph, adapter.state(), client.comments,
                workstream_id="GEN-37", predecessor_plan_revision="b" * 64,
                desired_scope=desired_scope,
                seeds=manifest["terminal_child_evidence_seeds"],
                desired_contracts=desired_contracts,
            )
        )
        manifest["terminal_child_evidence_seed_predecessor"] = binding
        seeded = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=HEAD,
            comments=client.comments,
        )
        predecessor_head = "2" * 40
        stale_projection = deepcopy(seeded["projection"])
        stale_scope = next(
            item["value"] for item in stale_projection
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        stale_primary = next(
            repository for repository in stale_scope["repositories"]
            if repository_key(repository) == stale_scope["primary_repository"]
        )
        self.assertEqual(stale_primary["exact_head"], HEAD)
        stale_primary["exact_head"] = predecessor_head
        for index, item in enumerate(stale_projection):
            adapter.append(build_projection_event(
                workstream_id="GEN-37", kind=item["kind"], key=item["key"],
                value=item["value"], plan_revision=PLAN,
                expected_revision=adapter.state().revision,
                created_at=f"2026-09-01T09:{index:02d}:00Z",
                authority=AUTHORITY,
            ))
        old_disposition = build_projection_event(
            workstream_id="GEN-37", kind="disposition", key="root",
            value={
                "disposition": "attach", "remote_head": predecessor_head,
                "recovered_from_checkpoint": None,
            },
            plan_revision=PLAN, expected_revision=adapter.state().revision,
            created_at="2026-09-01T09:19:00Z", authority=AUTHORITY,
        )
        adapter.append(old_disposition)
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="disposition", key="root",
            value={
                "disposition": "attach", "remote_head": HEAD,
                "recovered_from_checkpoint": None,
            },
            plan_revision=PLAN, expected_revision=adapter.state().revision,
            created_at="2026-09-01T09:20:00Z", authority=AUTHORITY,
            supersedes_event_id=old_disposition["event_id"],
        ))

        active = workstream_projection._active_heads(adapter.state())
        stale_scope_event = active[("scope", "root")]
        desired_scope = next(
            item["value"] for item in seeded["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        secondary_before = {
            repository_key(repository): repository["exact_head"]
            for repository in stale_scope_event["value"]["repositories"]
            if repository_key(repository)
            != stale_scope_event["value"]["primary_repository"]
        }
        evidence_before = {
            key: (event["event_id"], deepcopy(event["value"]))
            for (kind, key), event in active.items()
            if kind == "evidence_contract"
        }
        history_before = deepcopy(adapter.state().events)
        transition = {
            "repository_key": desired_scope["primary_repository"],
            "from_exact_head": predecessor_head,
            "to_exact_head": HEAD,
            "from_scope_event_id": stale_scope_event["event_id"],
            "from_scope_value_sha256": canonical_digest(
                stale_scope_event["value"]
            ),
            "from_disposition_event_id": old_disposition["event_id"],
            "from_disposition_value_sha256": canonical_digest(
                old_disposition["value"]
            ),
            "disposition": {
                "disposition": "attach", "remote_head": HEAD,
                "recovered_from_checkpoint": None,
            },
            "input_frontier_sha256": binding["input_frontier_sha256"],
        }
        combined = {
            **reviewed_manifest(adapter, deepcopy(seeded["projection"])),
            "terminal_child_evidence_seeds": deepcopy(
                manifest["terminal_child_evidence_seeds"]
            ),
            "terminal_child_evidence_seed_predecessor": deepcopy(binding),
            "terminal_child_evidence_seed_head_transition": transition,
        }
        mismatched = deepcopy(combined)
        mismatched["terminal_child_evidence_seed_head_transition"][
            "input_frontier_sha256"
        ] = "f" * 64
        before_comments = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_input_frontier_mismatch",
        ):
            prepare_terminal_child_evidence_seeds(
                mismatched, graph, adapter.state(), remote_head=HEAD,
                comments=client.comments,
            )
        self.assertEqual(len(client.comments), before_comments)

        prepared = prepare_terminal_child_evidence_seeds(
            combined, graph, adapter.state(), remote_head=HEAD,
            comments=client.comments,
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        readbacks = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in prepared["terminal_child_evidence_seeds"]
        }
        seed_revision = adapter.state().revision
        seed_result = reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-09-01T09:21:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: readbacks[child_id] for child_id in child_ids
            },
            projection_input_fence=lambda: binding["input_frontier_sha256"],
            checkpoint_fence=lambda: None,
            projection_comments=client.comments,
            projection_input_snapshot=graph,
            legacy_unresolved_relation_heads=unresolved,
        )
        self.assertFalse(seed_result["resume_authority_verified"])
        self.assertEqual(
            [
                (event["kind"], event["key"])
                for event in adapter.state().events[seed_revision:]
            ],
            [("scope", "root")],
        )

        active = workstream_projection._active_heads(adapter.state())
        current_scope = active[("scope", "root")]["value"]
        current_primary = next(
            repository for repository in current_scope["repositories"]
            if repository_key(repository) == current_scope["primary_repository"]
        )
        self.assertEqual(current_primary["exact_head"], HEAD)
        self.assertEqual({
            repository_key(repository): repository["exact_head"]
            for repository in current_scope["repositories"]
            if repository_key(repository) != current_scope["primary_repository"]
        }, secondary_before)
        self.assertEqual({
            key: (event["event_id"], event["value"])
            for (kind, key), event in active.items()
            if kind == "evidence_contract"
        }, evidence_before)

        repairs = []
        for child in children:
            evidence_event = next(
                event for (kind, _key), event in active.items()
                if kind == "evidence_contract"
                and event["value"]["owning_child"] == child["identifier"]
            )
            repairs.append({
                "child_identifier": child["identifier"],
                "child_issue_id": child["id"],
                "expected_child_readback_sha256": readbacks[
                    child["identifier"]
                ],
                "expected_assignee_id": child["assignee"]["id"],
                "approved_evidence_heads": [{
                    "key": evidence_event["key"],
                    "event_id": evidence_event["event_id"],
                    "value_sha256": canonical_digest(evidence_event["value"]),
                }],
            })
        repair_projection = [
            {"kind": kind, "key": key, "value": deepcopy(event["value"])}
            for (kind, key), event in sorted(active.items())
            if kind != "disposition"
        ]
        repair_manifest = {
            **reviewed_manifest(adapter, repair_projection),
            "terminal_child_repairs": repairs,
        }
        prepared_repair = prepare_terminal_child_repairs(
            repair_manifest, graph, adapter.state(),
        )
        repair_preview, repair_unresolved = (
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", prepared_repair, adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        )
        repair_result = reconcile_required_projection(
            adapter, repair_preview, prepared_repair, remote_head=HEAD,
            created_at="2026-09-01T09:22:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: readbacks[child_id] for child_id in child_ids
            },
            projection_input_fence=lambda: binding["input_frontier_sha256"],
            checkpoint_fence=lambda: None,
            legacy_unresolved_relation_heads=repair_unresolved,
        )
        self.assertTrue(repair_result["resume_authority_verified"])
        final_active = workstream_projection._active_heads(adapter.state())
        self.assertEqual(
            adapter.state().events[:len(history_before)], history_before,
        )
        self.assertEqual(
            {key for kind, key in final_active if kind == "child_closure"},
            {child["identifier"] for child in children},
        )
        self.assertEqual({
            key: (event["event_id"], event["value"])
            for (kind, key), event in final_active.items()
            if kind == "evidence_contract"
        }, evidence_before)
        final_scope = final_active[("scope", "root")]["value"]
        self.assertEqual({
            repository_key(repository): repository["exact_head"]
            for repository in final_scope["repositories"]
            if repository_key(repository) != final_scope["primary_repository"]
        }, secondary_before)

    def test_terminal_repair_consumer_refuses_primary_head_mismatch(self):
        client, adapter, source, graph, _children, manifest = (
            self.multi_terminal_repair_fixture(evidence_active=True)
        )
        prepared = prepare_terminal_child_repairs(
            manifest, graph, adapter.state(),
        )
        scope = next(
            item["value"] for item in prepared["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        primary = next(
            repository for repository in scope["repositories"]
            if repository_key(repository) == scope["primary_repository"]
        )
        primary["exact_head"] = "b" * 40
        comments_before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_repair_primary_head_mismatch",
        ):
            reconcile_required_projection(
                adapter, graph, prepared, remote_head=HEAD,
                created_at="2026-09-01T09:31:00Z",
                authenticated_source=source,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), comments_before)

    def test_terminal_seed_head_transition_recovers_before_scope_commit(self):
        client, adapter, source, graph, _children, manifest, new_head = (
            self.terminal_seed_head_transition_fixture()
        )
        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=new_head,
        )
        replay_prefix = [
            item for item in prepared["projection"]
            if item["kind"] == "evidence_contract"
        ]
        replay_prefix.append({
            "kind": "disposition", "key": "root", "value": {
                "disposition": "create_successor", "remote_head": new_head,
                "recovered_from_checkpoint": None,
            },
        })
        for item in replay_prefix:
            latest = next((
                event for event in reversed(adapter.state().events)
                if (event["kind"], event["key"])
                == (item["kind"], item["key"])
            ), None)
            adapter.append(build_projection_event(
                workstream_id="GEN-37", kind=item["kind"], key=item["key"],
                value=item["value"], plan_revision=PLAN,
                expected_revision=adapter.state().revision,
                created_at="2026-08-29T21:00:00Z",
                supersedes_event_id=(latest or {}).get("event_id"),
                authority=AUTHORITY,
            ))
        resumed = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=new_head,
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", resumed, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=new_head,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        before_revision = adapter.state().revision
        reconcile_required_projection(
            adapter, preview, resumed, remote_head=new_head,
            created_at="2026-08-29T21:01:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected[child_id] for child_id in child_ids
            },
            projection_input_fence=lambda: resumed[
                "terminal_child_evidence_seed_head_transition"
            ]["input_frontier_sha256"],
            legacy_unresolved_relation_heads=unresolved,
        )
        self.assertEqual(
            [(event["kind"], event["key"])
             for event in adapter.state().events[before_revision:]],
            [("scope", "root")],
        )

    def test_gen14_legacy_split_prefix_replays_exact_disposition_then_scope(self):
        class Gen14ProjectionClient(FakeProjectionClient):
            def execute(self, query, variables):
                response = super().execute(query, variables)
                if "query WorkstreamDeltaComments" in query:
                    response["issue"]["identifier"] = "GEN-14"
                return response

        plan = "e" * 64
        stored_frontier, frontier = "d" * 64, "f" * 64
        old_disposition_head, old_scope_head, new_head = (
            "a" * 40, "b" * 40, "c" * 40,
        )
        prefix_time = "2030-01-01T00:00:00Z"
        predecessor_plan = "f" * 64
        old_scope = {
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
            "child_ownership": {
                "GEN-1": "github.com:id:R_synthetic",
                "GEN-2": "github.com:id:R_synthetic",
            }, "linear": deepcopy(scope()["linear"]),
            "namespace": "synthetic",
        }
        children = [{
            "identifier": child,
            "id": f"00000000-0000-4000-8000-00000000000{index}",
            "parent": {"id": ROOT_UUID},
            "team": {
                "id": AUTHORITY["team_id"],
                "organization": {"id": AUTHORITY["workspace_id"]},
            },
            "project": {"id": AUTHORITY["project_id"]},
            "assignee": None, "state_id": f"state-{index}",
            "status": "Done", "status_type": "completed",
        } for index, child in enumerate(("GEN-1", "GEN-2"), start=1)]
        snapshot = {
            "root": {"id": ROOT_UUID, "identifier": "GEN-14", "revision": 1},
            "children": children,
        }
        predecessor_client = Gen14ProjectionClient()
        predecessor_adapter = LinearProjectionAdapter(
            predecessor_client, issue_id="GEN-14", workstream_id="GEN-14",
            plan_revision=predecessor_plan, **AUTHORITY,
        )

        def append_predecessor(kind, key, value):
            predecessor_adapter.append(build_projection_event(
                workstream_id="GEN-14", kind=kind, key=key, value=value,
                plan_revision=predecessor_plan,
                expected_revision=predecessor_adapter.state().revision,
                created_at="2029-12-31T22:00:00Z", authority=AUTHORITY,
            ))

        append_predecessor("scope", "root", deepcopy(old_scope))
        append_predecessor("source", "root", {
            "identity": "https://example.test/predecessor",
            "sha256": predecessor_plan,
        })
        append_predecessor("provenance", "synthetic", {
            "agent": "synthetic", "machine": "predecessor",
            "session_id": "predecessor",
        })
        append_predecessor("disposition", "root", {
            "disposition": "create_successor", "remote_head": old_scope_head,
            "recovered_from_checkpoint": None,
        })
        predecessor_contracts = {}
        for key, child in zip(("one", "two"), children):
            contract_value = evidence_contract()
            contract_value.update({
                "slice_id": key, "owning_child": child["identifier"],
                "repository": "github.com/acme/synthetic",
                "repository_key": old_scope["primary_repository"],
                "plan_revision": predecessor_plan,
                "exact_head": old_scope_head,
            })
            for layer in contract_value["layers"].values():
                for receipt in layer.get("receipts", []):
                    receipt["repository_key"] = contract_value["repository_key"]
                    receipt["exact_head"] = contract_value["exact_head"]
            append_predecessor("evidence_contract", key, contract_value)
            evidence_event = predecessor_adapter.state().events[-1]
            readback = terminal_child_readback(child)
            append_predecessor("child_closure", child["identifier"], {
                "schema_version": 2, **readback,
                "plan_revision": predecessor_plan,
                "repository_key": contract_value["repository_key"],
                "exact_head": old_scope_head,
                "evidence_heads": [{
                    "key": key, "event_id": evidence_event["event_id"],
                    "value_sha256": canonical_digest(evidence_event["value"]),
                }],
                "evidence_receipts_sha256": evidence_receipts_sha256([
                    contract_value,
                ]),
                "child_readback_sha256": canonical_digest(readback),
            })
            predecessor_contracts[key] = contract_value
        predecessor_events = list(predecessor_adapter.state().events)
        material = Delta(
            "gen14-predecessor", "GEN-14", "requirement", "agent",
            {"text": "Reconcile the captured successor."}, 0,
            "2029-12-31T22:30:00Z",
        )
        checkpoint = build_checkpoint(
            workstream_id="GEN-14", boundary_id="gen14-predecessor",
            root_revision=1, plan_revision=predecessor_plan,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "synthetic", "provider": "test",
                "session_id": "predecessor", "machine": "test",
                "worktree": {
                    "state": "safe", "path": "/worktree",
                    "branch": "predecessor", "head": old_scope_head,
                },
            },
            exact_head=old_scope_head, evidence=[], blocker=None,
            next_action="Reconcile the captured successor.",
        )
        supporting_comments = [{
            "id": "material-gen14-predecessor",
            "body": encode_event_comment(material),
            "createdAt": "2029-12-31T22:31:00Z",
            "updatedAt": "2029-12-31T22:31:00Z",
        }, {
            "id": "checkpoint-gen14-predecessor",
            "body": encode_checkpoint_comment(checkpoint),
            "createdAt": "2029-12-31T22:32:00Z",
            "updatedAt": "2029-12-31T22:32:00Z",
        }]
        predecessor_client.comments.extend(deepcopy(supporting_comments))
        target_before_prefix = LinearProjectionAdapter(
            predecessor_client, issue_id="GEN-14", workstream_id="GEN-14",
            plan_revision=plan, **AUTHORITY,
        )
        desired_scope = deepcopy(old_scope)
        next(repository for repository in desired_scope["repositories"]
             if repository_key(repository) == desired_scope["primary_repository"]
             )["exact_head"] = new_head
        seeds = [{
            "child_identifier": child["identifier"],
            "child_issue_id": child["id"],
            "expected_child_readback_sha256": canonical_digest(
                terminal_child_readback(child)
            ),
            "expected_assignee_id": None, "evidence_keys": [key],
        } for key, child in zip(("one", "two"), children)]
        desired_contracts = {}
        for key, contract_value in predecessor_contracts.items():
            desired_contracts[key] = deepcopy(contract_value)
            desired_contracts[key]["plan_revision"] = plan
        predecessor, _authorities = terminal_child_evidence_seed_predecessor_contract(
            snapshot, target_before_prefix.state(), predecessor_client.comments,
            workstream_id="GEN-14",
            predecessor_plan_revision=predecessor_plan,
            desired_scope=desired_scope, seeds=seeds,
            desired_contracts=desired_contracts,
        )
        frontier = predecessor["input_frontier_sha256"]
        common_authority = {
            "schema_version": 1,
            "predecessor_plan_revision": predecessor["plan_revision"],
            "predecessor_projection_revision": predecessor[
                "projection_revision"
            ],
            "predecessor_projection_events_sha256": predecessor[
                "projection_events_sha256"
            ],
            "predecessor_projection_frontier_event_id": predecessor[
                "projection_frontier_event_id"
            ],
            "predecessor_projection_frontier_sha256": predecessor[
                "projection_frontier_sha256"
            ],
            "projection_history_sha256": predecessor[
                "projection_history_sha256"
            ],
            "material_revision": predecessor["material_revision"],
            "material_events_sha256": predecessor["material_events_sha256"],
            "checkpoint_event_id": predecessor["checkpoint_event_id"],
            "checkpoint_events_sha256": predecessor[
                "checkpoint_events_sha256"
            ],
            "input_frontier_sha256": predecessor["input_frontier_sha256"],
        }
        evidence_values = []
        for head in predecessor["evidence_heads"]:
            contract_value = deepcopy(desired_contracts[head["key"]])
            contract_value.update({
                "predecessor_closure_authority": {
                **common_authority,
                "input_frontier_sha256": stored_frontier,
                "predecessor_evidence_event_id": head["evidence_event_id"],
                "predecessor_evidence_value_sha256": head[
                    "evidence_value_sha256"
                ],
                "predecessor_closure_event_id": head["closure_event_id"],
                "predecessor_closure_value_sha256": head[
                    "closure_value_sha256"
                ],
            },
            })
            for layer in contract_value["layers"].values():
                for receipt in layer.get("receipts", []):
                    receipt["repository_key"] = contract_value["repository_key"]
                    receipt["exact_head"] = contract_value["exact_head"]
            evidence_values.append(contract_value)
        old_source_identity = (
            "https://github.com/acme/plans/blob/"
            + "1" * 40 + "/plan.md"
        )
        target_source_identity = (
            "https://github.com/acme/plans/blob/"
            + "2" * 40 + "/plan.md"
        )
        values = evidence_values + [
            deepcopy(predecessor_events[2]["value"]),
            {"identity": old_source_identity, "sha256": plan},
            {"disposition": "create_successor",
             "remote_head": old_disposition_head,
             "recovered_from_checkpoint": None}, old_scope,
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
        original_digest = workstream_projection.GEN14_SPLIT_PREFIX_SHA256
        workstream_projection.GEN14_SPLIT_PREFIX_SHA256 = canonical_digest(events)
        original_generation_digest = (
            workstream_generation.GEN14_LEGACY_SPLIT_PREFIX_SHA256
        )
        workstream_generation.GEN14_LEGACY_SPLIT_PREFIX_SHA256 = (
            canonical_digest(events)
        )
        original_projection_stored = (
            workstream_projection.GEN14_SPLIT_STORED_FRONTIER_SHA256
        )
        original_projection_recomputed = (
            workstream_projection.GEN14_SPLIT_RECOMPUTED_FRONTIER_SHA256
        )
        original_generation_stored = (
            workstream_generation.GEN14_LEGACY_SPLIT_STORED_FRONTIER_SHA256
        )
        original_generation_recomputed = (
            workstream_generation.GEN14_LEGACY_SPLIT_RECOMPUTED_FRONTIER_SHA256
        )
        workstream_projection.GEN14_SPLIT_STORED_FRONTIER_SHA256 = (
            stored_frontier
        )
        workstream_projection.GEN14_SPLIT_RECOMPUTED_FRONTIER_SHA256 = frontier
        workstream_generation.GEN14_LEGACY_SPLIT_STORED_FRONTIER_SHA256 = (
            stored_frontier
        )
        workstream_generation.GEN14_LEGACY_SPLIT_RECOMPUTED_FRONTIER_SHA256 = (
            frontier
        )
        self.addCleanup(
            setattr, workstream_projection, "GEN14_SPLIT_PREFIX_SHA256",
            original_digest,
        )
        self.addCleanup(
            setattr, workstream_generation,
            "GEN14_LEGACY_SPLIT_PREFIX_SHA256", original_generation_digest,
        )
        self.addCleanup(
            setattr, workstream_projection,
            "GEN14_SPLIT_STORED_FRONTIER_SHA256", original_projection_stored,
        )
        self.addCleanup(
            setattr, workstream_projection,
            "GEN14_SPLIT_RECOMPUTED_FRONTIER_SHA256",
            original_projection_recomputed,
        )
        self.addCleanup(
            setattr, workstream_generation,
            "GEN14_LEGACY_SPLIT_STORED_FRONTIER_SHA256",
            original_generation_stored,
        )
        self.addCleanup(
            setattr, workstream_generation,
            "GEN14_LEGACY_SPLIT_RECOMPUTED_FRONTIER_SHA256",
            original_generation_recomputed,
        )
        state = SimpleNamespace(revision=6, events=events, snapshot={})
        contract = projection_review_contract(state)
        desired_scope = deepcopy(old_scope)
        primary = next(
            item for item in desired_scope["repositories"]
            if repository_key(item) == desired_scope["primary_repository"]
        )
        primary["exact_head"] = new_head
        transition = {
            "repository_key": desired_scope["primary_repository"],
            "from_exact_head": old_scope_head,
            "from_disposition_exact_head": old_disposition_head,
            "to_exact_head": new_head,
            "from_scope_event_id": events[5]["event_id"],
            "from_scope_value_sha256": canonical_digest(events[5]["value"]),
            "from_disposition_event_id": events[4]["event_id"],
            "from_disposition_value_sha256": canonical_digest(events[4]["value"]),
            "disposition": {
                "disposition": "create_successor",
                "remote_head": new_head,
                "recovered_from_checkpoint": None,
            },
            "input_frontier_sha256": predecessor["input_frontier_sha256"],
            "created_at": "2026-09-01T10:00:00Z",
        }
        manifest = {
            **contract,
            "projection": [
                {"kind": event["kind"], "key": event["key"],
                 "value": desired_scope if event["kind"] == "scope"
                 else deepcopy(event["value"])}
                for event in events if event["kind"] != "disposition"
            ],
            "retirements": [],
            "terminal_child_evidence_seeds": deepcopy(seeds),
            "terminal_child_evidence_seed_predecessor": predecessor,
            "terminal_child_evidence_seed_legacy_split_head_repair": transition,
        }
        workstream_projection._reviewed_manifest(manifest)
        workstream_projection._validate_gen14_legacy_split_repair_prefix(
            manifest, state, desired_scope,
        )
        disposition = build_projection_event(
            workstream_id="GEN-14", kind="disposition", key="root",
            value=transition["disposition"],
            plan_revision=plan,
            expected_revision=6, created_at=transition["created_at"],
            supersedes_event_id=events[4]["event_id"],
            authority=AUTHORITY,
        )
        repair_scope_event = build_projection_event(
            workstream_id="GEN-14", kind="scope", key="root",
            value=desired_scope,
            plan_revision=plan,
            expected_revision=7, created_at=transition["created_at"],
            supersedes_event_id=events[5]["event_id"],
            authority=AUTHORITY,
        )
        for tail in ([disposition], [disposition, repair_scope_event]):
            replay = SimpleNamespace(
                revision=6 + len(tail), events=[*events, *tail], snapshot={},
            )
            workstream_projection._validate_gen14_legacy_split_repair_prefix(
                manifest, replay, desired_scope,
            )
            regenerated = deepcopy(manifest)
            regenerated.update(projection_review_contract(replay))
            workstream_projection._validate_gen14_legacy_split_repair_prefix(
                regenerated, replay, desired_scope,
            )
        self.assertEqual(disposition["supersedes_event_id"], events[4]["event_id"])
        self.assertEqual(
            repair_scope_event["supersedes_event_id"], events[5]["event_id"],
        )

        source = {"identity": target_source_identity, "sha256": plan}
        readbacks = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        def adapter_with_tail(tail, *, prefix_events=events, extra_comments=()):
            client = Gen14ProjectionClient()
            client.comments = [
                {
                    **projection_comment(event),
                    "createdAt": f"2030-01-01T00:00:{index:02d}Z",
                    "updatedAt": f"2030-01-01T00:00:{index:02d}Z",
                }
                for index, event in enumerate(
                    [*predecessor_events, *prefix_events, *tail]
                )
            ] + deepcopy(supporting_comments) + deepcopy(list(extra_comments))
            return client, LinearProjectionAdapter(
                client, issue_id="GEN-14", workstream_id="GEN-14",
                plan_revision=plan, **AUTHORITY,
            )

        def prepared_operator_contract(adapter, requested_head, requested_at):
            return prepare_generation_operator_contract(
                comments=deepcopy(adapter.client.comments),
                graph=deepcopy(snapshot), workstream_id="GEN-14",
                authority=AUTHORITY,
                description_plan_revision=predecessor_plan,
                target_source=source, created_at=requested_at,
                remote_head=requested_head,
                started_state={
                    "id": "state-started", "name": "In Progress",
                    "type": "started", "team_id": AUTHORITY["team_id"],
                },
            )

        def apply_operator_contract(adapter, operator):
            preview = operator["projection_preview"]
            invocation = deepcopy(preview["invocation"])
            reviewed_source = invocation.pop("source")
            return reconcile_required_projection(
                adapter, snapshot, preview["manifest"],
                **invocation, authenticated_source=reviewed_source,
                terminal_child_fence=lambda child_ids: {
                    child_id: readbacks[child_id] for child_id in child_ids
                },
                projection_input_fence=lambda: frontier,
                checkpoint_fence=lambda: None,
                projection_comments=adapter.client.comments,
                projection_input_snapshot=snapshot,
            )

        _fresh_client, fresh_adapter = adapter_with_tail([])
        self.assertTrue(all(
            {"id", "body", "createdAt", "updatedAt"} <= set(comment)
            for comment in _fresh_client.comments
        ))
        decoded_prefix = list(fresh_adapter.state().events[:6])
        self.assertEqual(decoded_prefix, events)
        self.assertEqual(canonical_digest(decoded_prefix), canonical_digest(events))
        fresh_operator = prepared_operator_contract(
            fresh_adapter, new_head, transition["created_at"],
        )
        self.assertEqual(
            fresh_operator["projection_preview"]["manifest"][
                "terminal_child_evidence_seed_predecessor"
            ],
            predecessor,
        )
        self.assertEqual(
            fresh_operator["projection_preview"]["manifest"][
                "terminal_child_evidence_seed_legacy_split_head_repair"
            ]["input_frontier_sha256"],
            frontier,
        )
        fresh_before = fresh_adapter.state().revision
        fresh_result = apply_operator_contract(fresh_adapter, fresh_operator)
        self.assertEqual(
            [(event["kind"], event["key"])
             for event in fresh_adapter.state().events[fresh_before:]],
            [("disposition", "root"), ("scope", "root")],
        )
        self.assertEqual(len(fresh_result["writes"]), 2)

        def rebuilt_prefix(*, value_updates=None, plan_updates=None,
                           authority_updates=None):
            value_updates = value_updates or {}
            plan_updates = plan_updates or {}
            authority_updates = authority_updates or {}
            return [build_projection_event(
                workstream_id="GEN-14", kind=event["kind"], key=event["key"],
                value=deepcopy(value_updates.get(index, event["value"])),
                plan_revision=plan_updates.get(index, event["plan_revision"]),
                expected_revision=index, created_at=event["created_at"],
                authority=deepcopy(authority_updates.get(
                    index, event["authority"],
                )),
            ) for index, event in enumerate(events)]

        def assert_decoded_refusal(prefix_events, operator, error,
                                   *, prefix_digest=None):
            bad_client, bad_adapter = adapter_with_tail(
                [], prefix_events=prefix_events,
            )
            writes_before = sum(
                "mutation " in query for query, _variables in bad_client.calls
            )
            patcher = (
                mock.patch.object(
                    workstream_projection, "GEN14_SPLIT_PREFIX_SHA256",
                    prefix_digest,
                ) if prefix_digest is not None else nullcontext()
            )
            with patcher, self.assertRaisesRegex(
                (LinearProjectionError, ValueError), error,
            ):
                apply_operator_contract(bad_adapter, operator)
            self.assertEqual(sum(
                "mutation " in query for query, _variables in bad_client.calls
            ), writes_before)

        def assert_decoded_producer_refusal(
            prefix_events, requested_head, error, *, prefix_digest=None,
            extra_comments=(),
        ):
            bad_client, bad_adapter = adapter_with_tail(
                [], prefix_events=prefix_events, extra_comments=extra_comments,
            )
            writes_before = sum(
                "mutation " in query for query, _variables in bad_client.calls
            )
            patcher = (
                mock.patch.object(
                    workstream_generation,
                    "GEN14_LEGACY_SPLIT_PREFIX_SHA256", prefix_digest,
                ) if prefix_digest is not None else nullcontext()
            )
            with patcher, self.assertRaisesRegex(
                (
                    LinearProjectionError,
                    workstream_generation.WorkstreamGenerationError,
                    ValueError,
                ),
                error,
            ):
                prepared_operator_contract(
                    bad_adapter, requested_head, transition["created_at"],
                )
            self.assertEqual(sum(
                "mutation " in query for query, _variables in bad_client.calls
            ), writes_before)

        wrong_prefix_value = deepcopy(events[2]["value"])
        wrong_prefix_value["machine"] = "different-machine"
        wrong_prefix_events = rebuilt_prefix(value_updates={2: wrong_prefix_value})
        assert_decoded_producer_refusal(
            wrong_prefix_events, new_head,
            "prefix|unrelated|replacement|target_disposition",
        )
        assert_decoded_refusal(
            wrong_prefix_events, fresh_operator,
            "legacy_split_head_repair_prefix_mismatch",
        )

        wrong_stored_value_one = deepcopy(events[0]["value"])
        wrong_stored_value_two = deepcopy(events[1]["value"])
        for value in (wrong_stored_value_one, wrong_stored_value_two):
            value["predecessor_closure_authority"][
                "input_frontier_sha256"
            ] = "7" * 64
        wrong_stored_prefix = rebuilt_prefix(value_updates={
            0: wrong_stored_value_one, 1: wrong_stored_value_two,
        })
        assert_decoded_producer_refusal(
            wrong_stored_prefix, new_head,
            "generation_prepare_legacy_split_head_stored_frontier_changed",
            prefix_digest=canonical_digest(wrong_stored_prefix),
        )
        assert_decoded_refusal(
            wrong_stored_prefix, fresh_operator,
            "legacy_split_head_repair_stored_frontier_mismatch",
            prefix_digest=canonical_digest(wrong_stored_prefix),
        )

        wrong_recomputed_operator = deepcopy(fresh_operator)
        wrong_recomputed_operator["projection_preview"]["manifest"][
            "terminal_child_evidence_seed_legacy_split_head_repair"
        ]["input_frontier_sha256"] = "8" * 64
        wrong_recomputed_operator["projection_preview"]["manifest"][
            "terminal_child_evidence_seed_predecessor"
        ]["input_frontier_sha256"] = "8" * 64
        assert_decoded_refusal(
            events, wrong_recomputed_operator,
            "legacy_split_head_repair_recomputed_frontier_mismatch",
        )

        wrong_plan_prefix = rebuilt_prefix(plan_updates={0: "f" * 64})
        assert_decoded_producer_refusal(
            wrong_plan_prefix, new_head, "plan|revision|activation|prefix",
        )
        assert_decoded_refusal(
            wrong_plan_prefix, fresh_operator, "plan|revision|activation|prefix",
        )
        wrong_route = {**AUTHORITY, "project_id": "different-project"}
        wrong_route_prefix = rebuilt_prefix(authority_updates={0: wrong_route})
        assert_decoded_producer_refusal(
            wrong_route_prefix, new_head, "route|authority|prefix",
        )
        assert_decoded_refusal(
            wrong_route_prefix, fresh_operator, "route|authority|prefix",
        )

        reused_head_operator = deepcopy(fresh_operator)
        reused_head_operator["projection_preview"]["manifest"][
            "terminal_child_evidence_seed_legacy_split_head_repair"
        ]["to_exact_head"] = old_scope_head
        reused_head_operator["projection_preview"]["manifest"][
            "terminal_child_evidence_seed_legacy_split_head_repair"
        ]["disposition"]["remote_head"] = old_scope_head
        assert_decoded_refusal(
            events, reused_head_operator,
            "invalid_terminal_child_evidence_seed_legacy_split_head_repair",
        )
        assert_decoded_producer_refusal(
            events, old_scope_head, "head|transition|replacement|changed",
        )

        frontier_drift = Delta(
            "gen14-frontier-drift", "GEN-14", "requirement", "agent",
            {"text": "Concurrent material drift."}, 1,
            "2030-01-01T00:30:00Z",
        )
        assert_decoded_producer_refusal(
            events, new_head,
            "generation_prepare_legacy_split_head_recomputed_frontier_changed",
            extra_comments=[{
                "id": "material-gen14-frontier-drift",
                "body": encode_event_comment(frontier_drift),
                "createdAt": "2030-01-01T00:30:00Z",
                "updatedAt": "2030-01-01T00:30:00Z",
            }],
        )

        # Live-shaped stable-head seam: D6/S7 are both durable at the exact
        # requested head. Fresh prepare must retain the authenticated prefix
        # evidence values instead of proposing a predecessor-authority rewrite.
        stable_client, stable_adapter = adapter_with_tail([
            disposition, repair_scope_event,
        ])
        self.assertTrue(
            workstream_projection._gen14_completed_split_stable_source_prefix(
                stable_adapter.state()
            )
        )
        stable_operator = prepared_operator_contract(
            stable_adapter, new_head, "2030-01-01T03:00:00Z",
        )
        self.assertEqual(
            stable_operator["projection_preview"]["phase"],
            "terminal_source_transition",
        )
        stable_before = stable_adapter.state().revision
        stable_source_result = apply_operator_contract(
            stable_adapter, stable_operator,
        )
        self.assertEqual(len(stable_source_result["writes"]), 1)
        self.assertEqual(
            [(event["kind"], event["key"])
             for event in stable_adapter.state().events[stable_before:]],
            [("source", "root")],
        )
        stable_closure = prepared_operator_contract(
            stable_adapter, new_head, "2030-01-01T03:00:00Z",
        )
        self.assertEqual(
            stable_closure["projection_preview"]["phase"],
            "terminal_closure_repair",
        )
        stable_bridge = stable_closure["projection_preview"]["manifest"][
            "terminal_child_repair_gen14_frontier_bridge"
        ]
        stable_source_event = stable_adapter.state().events[8]
        self.assertEqual(stable_bridge, {
            "prefix_sha256": canonical_digest(events),
            "stored_input_frontier_sha256": stored_frontier,
            "recomputed_input_frontier_sha256": frontier,
            "source_event_id": stable_source_event["event_id"],
            "source_value_sha256": canonical_digest(
                stable_source_event["value"]
            ),
            "created_at": "2030-01-01T03:00:00Z",
            "child_identifiers": sorted(readbacks),
        })
        forged_bridge = deepcopy(stable_closure["projection_preview"]["manifest"])
        forged_bridge["terminal_child_repair_gen14_frontier_bridge"][
            "recomputed_input_frontier_sha256"
        ] = "9" * 64
        comments_before_bridge_refusal = len(stable_client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "invalid_terminal_child_repair_gen14_frontier_bridge",
        ):
            prepare_terminal_child_repairs(
                forged_bridge, snapshot, stable_adapter.state(),
            )
        self.assertEqual(
            len(stable_client.comments), comments_before_bridge_refusal,
        )

        closure_items = sorted(
            [
                item for item in stable_closure["projection_preview"][
                    "manifest"
                ]["projection"]
                if item["kind"] == "child_closure"
            ],
            key=lambda item: item["key"],
        )
        canonical_closure_events = [
            build_projection_event(
                workstream_id="GEN-14", kind="child_closure",
                key=item["key"], value=item["value"], plan_revision=plan,
                expected_revision=9 + index,
                created_at=stable_bridge["created_at"], authority=AUTHORITY,
            )
            for index, item in enumerate(closure_items)
        ]

        class DirectStateAdapter:
            def __init__(self, state, *, workstream_id="GEN-14"):
                self._state = state
                self.workstream_id = workstream_id
                self.plan_revision = plan
                self.authority = deepcopy(AUTHORITY)
                self.workspace_id = AUTHORITY["workspace_id"]
                self.team_id = AUTHORITY["team_id"]
                self.project_id = AUTHORITY["project_id"]
                self.root_issue_id = AUTHORITY["root_issue_id"]
                self.append_calls = 0

            def state(self):
                return self._state

            def append(self, *_args, **_kwargs):
                self.append_calls += 1
                raise AssertionError("bridge refusal must precede append")

        def assert_direct_bridge_refusal(
            tail, *, workstream_id="GEN-14",
            error="terminal_child_repair_gen14_frontier_bridge_mismatch",
            bridge_updates=None, source_override=None,
            closure_value_updates=None,
        ):
            forged_state = SimpleNamespace(
                revision=9 + len(tail),
                events=tuple([
                    *stable_adapter.state().events[:9], *tail,
                ]),
                snapshot=deepcopy(stable_adapter.state().snapshot),
            )
            direct_adapter = DirectStateAdapter(
                forged_state, workstream_id=workstream_id,
            )
            forged_manifest = deepcopy(
                stable_closure["projection_preview"]["manifest"]
            )
            forged_manifest.update(projection_review_contract(forged_state))
            forged_manifest[
                "terminal_child_repair_gen14_frontier_bridge"
            ].update(bridge_updates or {})
            if source_override is not None:
                next(
                    item for item in forged_manifest["projection"]
                    if (item["kind"], item["key"]) == ("source", "root")
                )["value"] = deepcopy(source_override)
            for item in forged_manifest["projection"]:
                if (
                    item["kind"] == "child_closure"
                    and item["key"] in (closure_value_updates or {})
                ):
                    item["value"] = deepcopy(
                        closure_value_updates[item["key"]]
                    )
            with self.assertRaisesRegex(LinearProjectionError, error):
                reconcile_required_projection(
                    direct_adapter, snapshot, forged_manifest,
                    remote_head=new_head,
                    created_at=stable_bridge["created_at"],
                    authenticated_source=source_override or source,
                    terminal_child_fence=lambda child_ids: {
                        child_id: readbacks[child_id]
                        for child_id in child_ids
                    },
                    projection_input_fence=lambda: frontier,
                    checkpoint_fence=lambda: None,
                    projection_input_snapshot=snapshot,
                    expected_projection_input_frontier=frontier,
                )
            self.assertEqual(direct_adapter.append_calls, 0)

        assert_direct_bridge_refusal([], workstream_id="GEN-15")
        assert_direct_bridge_refusal(
            [], bridge_updates={"source_event_id": "forged-source-event"},
        )
        assert_direct_bridge_refusal(
            [], bridge_updates={"created_at": "2030-01-01T03:00:01Z"},
        )
        assert_direct_bridge_refusal(
            [], error="terminal_child_repair_source_changed",
            source_override={
                "identity": "https://github.com/Generous-Corp/forged/blob/"
                f"{plan}/PLAN.md",
                "sha256": plan,
            },
        )
        assert_direct_bridge_refusal(list(reversed(canonical_closure_events)))
        forged_existing_value = deepcopy(closure_items[0]["value"])
        forged_existing_value["child_readback_sha256"] = "a" * 64
        forged_existing_closure = build_projection_event(
            workstream_id="GEN-14", kind="child_closure",
            key=closure_items[0]["key"], value=forged_existing_value,
            plan_revision=plan, expected_revision=9,
            created_at=stable_bridge["created_at"], authority=AUTHORITY,
        )
        assert_direct_bridge_refusal(
            [forged_existing_closure],
            error=(
                "terminal_child_repair_closure_conflict|"
                "terminal_child_repair_gen14_frontier_bridge_mismatch"
            ),
            closure_value_updates={
                closure_items[0]["key"]: forged_existing_value,
            },
        )
        no_op_scope = build_projection_event(
            workstream_id="GEN-14", kind="scope", key="root",
            value=deepcopy(stable_adapter.state().events[7]["value"]),
            plan_revision=plan, expected_revision=9,
            created_at=stable_bridge["created_at"],
            supersedes_event_id=stable_adapter.state().events[7]["event_id"],
            authority=AUTHORITY,
        )
        assert_direct_bridge_refusal([no_op_scope])
        duplicate_closure = build_projection_event(
            workstream_id="GEN-14", kind="child_closure",
            key=closure_items[0]["key"], value=closure_items[0]["value"],
            plan_revision=plan, expected_revision=10,
            created_at=stable_bridge["created_at"],
            supersedes_event_id=canonical_closure_events[0]["event_id"],
            authority=AUTHORITY,
        )
        assert_direct_bridge_refusal([
            canonical_closure_events[0], duplicate_closure,
        ])
        overlong_closure = build_projection_event(
            workstream_id="GEN-14", kind="child_closure",
            key=closure_items[0]["key"],
            value=closure_items[0]["value"], plan_revision=plan,
            expected_revision=11, created_at=stable_bridge["created_at"],
            supersedes_event_id=canonical_closure_events[0]["event_id"],
            authority=AUTHORITY,
        )
        assert_direct_bridge_refusal([
            *canonical_closure_events, overlong_closure,
        ])

        stable_closure_before = stable_adapter.state().revision
        apply_operator_contract(stable_adapter, stable_closure)
        self.assertEqual(
            [event["kind"] for event in stable_adapter.state().events[
                stable_closure_before:
            ]],
            ["child_closure", "child_closure"],
        )
        stable_continued = prepared_operator_contract(
            stable_adapter, new_head, "2030-01-01T03:00:00Z",
        )
        if stable_continued["projection_preview"]["phase"] != "activation_ready":
            self.assertEqual(
                stable_continued["projection_preview"]["phase"],
                "complete_projection",
            )
            apply_operator_contract(stable_adapter, stable_continued)
            stable_continued = prepared_operator_contract(
                stable_adapter, new_head, "2030-01-01T03:00:00Z",
            )
        self.assertEqual(
            stable_continued["projection_preview"]["phase"],
            "activation_ready",
        )
        self.assertFalse(any(
            event["kind"] in {"evidence_contract", "disposition", "scope"}
            for event in stable_adapter.state().events[stable_before:]
        ))
        self.assertGreater(len(stable_client.comments), 0)

        interrupted_client, interrupted_adapter = adapter_with_tail([disposition])
        newer_requested_head = "d" * 40
        interrupted_operator = prepared_operator_contract(
            interrupted_adapter, newer_requested_head,
            "2030-01-01T03:00:00Z",
        )
        self.assertEqual(interrupted_operator["remote_head"], newer_requested_head)
        self.assertEqual(
            interrupted_operator["projection_preview"]["invocation"], {
                "remote_head": new_head,
                "created_at": transition["created_at"],
                "source": {
                    "identity": old_source_identity, "sha256": plan,
                },
            },
        )
        interrupted_before = interrupted_adapter.state().revision
        interrupted_result = apply_operator_contract(
            interrupted_adapter, interrupted_operator,
        )
        self.assertEqual(
            [(event["kind"], event["key"])
             for event in interrupted_adapter.state().events[
                 interrupted_before:
             ]],
            [("scope", "root")],
        )
        self.assertEqual(len(interrupted_result["writes"]), 1)

        replay_operator = deepcopy(interrupted_operator)
        comments_before_replay = len(interrupted_client.comments)
        replay_result = apply_operator_contract(
            interrupted_adapter, replay_operator,
        )
        self.assertEqual(replay_result["writes"], [])
        self.assertEqual(len(interrupted_client.comments), comments_before_replay)

        later_created_at = "2030-01-01T05:00:00Z"
        later_operator = prepared_operator_contract(
            interrupted_adapter, newer_requested_head, later_created_at,
        )
        later_preview = later_operator["projection_preview"]
        self.assertEqual(later_operator["source"], source)
        self.assertEqual(later_preview["invocation"], {
            "remote_head": newer_requested_head,
            "created_at": later_created_at,
            "source": {
                "identity": old_source_identity, "sha256": plan,
            },
        })
        self.assertIn(
            "terminal_child_evidence_seed_head_transition",
            later_preview["manifest"],
        )
        self.assertEqual(
            later_preview["manifest"][
                "terminal_child_evidence_seed_head_transition"
            ]["created_at"],
            later_created_at,
        )
        self.assertNotIn(
            "terminal_child_evidence_seed_legacy_split_head_repair",
            later_preview["manifest"],
        )
        later_before = interrupted_adapter.state().revision
        later_result = apply_operator_contract(
            interrupted_adapter, later_operator,
        )
        later_tail = interrupted_adapter.state().events[later_before:]
        self.assertEqual(
            [(event["kind"], event["key"]) for event in later_tail],
            [
                ("evidence_contract", "one"),
                ("evidence_contract", "two"),
                ("disposition", "root"), ("scope", "root"),
            ],
        )
        self.assertEqual(len(later_result["writes"]), 4)
        self.assertTrue(all(
            event["value"]["predecessor_closure_authority"][
                "input_frontier_sha256"
            ] == frontier
            for event in later_tail[:2]
        ))
        comments_before_ordinary_replay = len(interrupted_client.comments)
        ordinary_replay = apply_operator_contract(
            interrupted_adapter, later_operator,
        )
        self.assertEqual(ordinary_replay["writes"], [])
        self.assertEqual(
            len(interrupted_client.comments), comments_before_ordinary_replay,
        )

        source_operator = prepared_operator_contract(
            interrupted_adapter, newer_requested_head, later_created_at,
        )
        self.assertEqual(
            source_operator["projection_preview"]["phase"],
            "terminal_source_transition",
        )
        self.assertEqual(
            source_operator["projection_preview"]["invocation"]["source"],
            source,
        )
        source_before = interrupted_adapter.state().revision
        source_result = apply_operator_contract(
            interrupted_adapter, source_operator,
        )
        self.assertEqual(len(source_result["writes"]), 1)
        self.assertEqual(
            [(event["kind"], event["key"])
             for event in interrupted_adapter.state().events[source_before:]],
            [("source", "root")],
        )
        closure_operator = prepared_operator_contract(
            interrupted_adapter, newer_requested_head, later_created_at,
        )
        self.assertEqual(
            closure_operator["projection_preview"]["phase"],
            "terminal_closure_repair",
        )
        closure_before = interrupted_adapter.state().revision
        closure_result = apply_operator_contract(
            interrupted_adapter, closure_operator,
        )
        self.assertGreater(len(closure_result["writes"]), 0)
        self.assertEqual(
            [event["kind"] for event in interrupted_adapter.state().events[
                closure_before:
            ]],
            ["child_closure", "child_closure"],
        )
        continued = prepared_operator_contract(
            interrupted_adapter, newer_requested_head, later_created_at,
        )
        if continued["projection_preview"]["phase"] != "activation_ready":
            self.assertEqual(
                continued["projection_preview"]["phase"],
                "complete_projection",
            )
            apply_operator_contract(interrupted_adapter, continued)
            continued = prepared_operator_contract(
                interrupted_adapter, newer_requested_head, later_created_at,
            )
        self.assertEqual(
            continued["projection_preview"]["phase"], "activation_ready",
        )

        for prefix_count in range(1, len(later_tail)):
            with self.subTest(ordinary_prefix_count=prefix_count):
                partial_client, partial_adapter = adapter_with_tail([
                    disposition, repair_scope_event, *later_tail[:prefix_count],
                ])
                partial_operator = prepared_operator_contract(
                    partial_adapter, newer_requested_head, later_created_at,
                )
                partial_result = apply_operator_contract(
                    partial_adapter, partial_operator,
                )
                self.assertEqual(
                    partial_adapter.state().events[8:], later_tail,
                )
                self.assertEqual(
                    len(partial_result["writes"]),
                    len(later_tail) - prefix_count,
                )

        bad_normalization_value = deepcopy(later_tail[0]["value"])
        bad_normalization_value["predecessor_closure_authority"][
            "input_frontier_sha256"
        ] = "9" * 64
        bad_normalization = build_projection_event(
            workstream_id="GEN-14", kind=later_tail[0]["kind"],
            key=later_tail[0]["key"], value=bad_normalization_value,
            plan_revision=plan, expected_revision=8,
            created_at=later_created_at,
            supersedes_event_id=events[0]["event_id"], authority=AUTHORITY,
        )
        bad_client, bad_adapter = adapter_with_tail([
            disposition, repair_scope_event, bad_normalization,
        ])
        with self.assertRaises(LinearProjectionError):
            prepared_operator_contract(
                bad_adapter, newer_requested_head, later_created_at,
            )
        self.assertFalse(any(
            "mutation " in query for query, _variables in bad_client.calls
        ))

        with mock.patch.object(
            workstream_projection, "prepare_terminal_child_repairs",
            side_effect=lambda repair_manifest, _snapshot, _state: (
                repair_manifest
            ),
        ):
            continued = prepared_operator_contract(
                interrupted_adapter, newer_requested_head,
                "2030-01-01T06:00:00Z",
            )
        self.assertNotIn(
            "terminal_child_evidence_seed_legacy_split_head_repair",
            continued["projection_preview"]["manifest"],
        )

        planted = {
            "extra_event": [*events, {**disposition, "kind": "choice"}],
            "wrong_disposition": [*events, {**disposition, "value": {
                **transition["disposition"], "disposition": "attach",
            }}],
            "wrong_order": [*events, repair_scope_event],
        }
        for name, bad_events in planted.items():
            with self.subTest(name=name), self.assertRaises(LinearProjectionError):
                workstream_projection._validate_gen14_legacy_split_repair_prefix(
                    manifest, SimpleNamespace(
                        revision=len(bad_events), events=bad_events, snapshot={},
                    ), desired_scope,
                )
        drifted = deepcopy(events)
        drifted[5]["value"]["namespace"] = "arbitrary-split"
        with self.assertRaisesRegex(
            LinearProjectionError, "legacy_split_head_repair_prefix_mismatch",
        ):
            workstream_projection._validate_gen14_legacy_split_repair_prefix(
                manifest, SimpleNamespace(revision=6, events=drifted, snapshot={}),
                desired_scope,
            )

        wrong_recomputed = deepcopy(manifest)
        wrong_recomputed[
            "terminal_child_evidence_seed_legacy_split_head_repair"
        ]["input_frontier_sha256"] = "8" * 64
        with mock.patch.object(
            workstream_projection, "GEN14_SPLIT_PREFIX_SHA256",
            canonical_digest(state.events[:6]),
        ), mock.patch.object(
            workstream_projection, "GEN14_SPLIT_STORED_FRONTIER_SHA256",
            stored_frontier,
        ), mock.patch.object(
            workstream_projection, "GEN14_SPLIT_RECOMPUTED_FRONTIER_SHA256",
            frontier,
        ), self.assertRaisesRegex(
            LinearProjectionError, "recomputed_frontier_mismatch",
        ):
            workstream_projection._validate_gen14_legacy_split_repair_prefix(
                wrong_recomputed, state, desired_scope,
            )

        wrong_stored_events = deepcopy(events)
        for event in wrong_stored_events[:2]:
            event["value"]["predecessor_closure_authority"][
                "input_frontier_sha256"
            ] = "7" * 64
        with mock.patch.object(
            workstream_projection, "GEN14_SPLIT_PREFIX_SHA256",
            canonical_digest(wrong_stored_events),
        ), mock.patch.object(
            workstream_projection, "GEN14_SPLIT_STORED_FRONTIER_SHA256",
            stored_frontier,
        ), mock.patch.object(
            workstream_projection, "GEN14_SPLIT_RECOMPUTED_FRONTIER_SHA256",
            frontier,
        ), self.assertRaisesRegex(
            LinearProjectionError, "stored_frontier_mismatch",
        ):
            workstream_projection._validate_gen14_legacy_split_repair_prefix(
                manifest, SimpleNamespace(
                    revision=6, events=wrong_stored_events, snapshot={},
                ), desired_scope,
            )

    def test_terminal_seed_head_transition_recovers_each_evidence_prefix(self):
        for prefix_count, expected_kinds in (
            (1, [
                ("evidence_contract", "gen-72-terminal"),
                ("disposition", "root"), ("scope", "root"),
            ]),
            (2, [("disposition", "root"), ("scope", "root")]),
        ):
            with self.subTest(prefix_count=prefix_count):
                client, adapter, source, graph, _children, manifest, new_head = (
                    self.terminal_seed_head_transition_fixture()
                )
                prepared = prepare_terminal_child_evidence_seeds(
                    manifest, graph, adapter.state(), remote_head=new_head,
                )
                evidence_items = [
                    item for item in prepared["projection"]
                    if item["kind"] == "evidence_contract"
                ]
                for item in evidence_items[:prefix_count]:
                    adapter.append(build_projection_event(
                        workstream_id="GEN-37", kind=item["kind"],
                        key=item["key"], value=item["value"],
                        plan_revision=PLAN,
                        expected_revision=adapter.state().revision,
                        created_at="2026-08-29T21:00:00Z",
                        authority=AUTHORITY,
                    ))
                resumed = prepare_terminal_child_evidence_seeds(
                    manifest, graph, adapter.state(), remote_head=new_head,
                )
                preview, unresolved = (
                    load_material_history_for_projection_reconcile(
                        graph, client.comments, "GEN-37", resumed, adapter,
                        authenticated_route=AUTHORITY,
                        authenticated_source=source, remote_head=new_head,
                        relation_target_resolver=self.relation_target_resolver,
                    )
                )
                expected = {
                    seed["child_identifier"]:
                    seed["expected_child_readback_sha256"]
                    for seed in manifest["terminal_child_evidence_seeds"]
                }
                before_revision = adapter.state().revision
                reconcile_required_projection(
                    adapter, preview, resumed, remote_head=new_head,
                    created_at="2026-08-29T21:01:00Z",
                    authenticated_source=source,
                    relation_target_resolver=self.relation_target_resolver,
                    terminal_child_fence=lambda child_ids: {
                        child_id: expected[child_id] for child_id in child_ids
                    },
                    projection_input_fence=lambda: resumed[
                        "terminal_child_evidence_seed_head_transition"
                    ]["input_frontier_sha256"],
                    legacy_unresolved_relation_heads=unresolved,
                )
                self.assertEqual(
                    [(event["kind"], event["key"])
                     for event in adapter.state().events[before_revision:]],
                    expected_kinds,
                )

    def test_terminal_seed_head_transition_refuses_unrelated_scope_or_head(self):
        client, adapter, source, graph, _children, manifest, new_head = (
            self.terminal_seed_head_transition_fixture()
        )
        changed_namespace = deepcopy(manifest)
        next(
            item for item in changed_namespace["projection"]
            if item["kind"] == "scope"
        )["value"]["namespace"] = "unrelated-change"
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_scope_head_only_required",
        ):
            prepare_terminal_child_evidence_seeds(
                changed_namespace, graph, adapter.state(), remote_head=new_head,
            )

        wrong_disposition_binding = deepcopy(manifest)
        wrong_disposition_binding[
            "terminal_child_evidence_seed_head_transition"
        ]["from_disposition_value_sha256"] = "0" * 64
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_reviewed_predecessor_missing",
        ):
            prepare_terminal_child_evidence_seeds(
                wrong_disposition_binding, graph, adapter.state(),
                remote_head=new_head,
            )

        wrong_head = deepcopy(manifest)
        next(
            item for item in wrong_head["projection"]
            if item["kind"] == "scope"
        )["value"]["repositories"][0]["exact_head"] = "c" * 40
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_primary_head_transition_invalid",
        ):
            prepare_terminal_child_evidence_seeds(
                wrong_head, graph, adapter.state(), remote_head=new_head,
            )

        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(), remote_head=new_head,
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=new_head,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        bad_disposition = deepcopy(prepared)
        bad_disposition[
            "terminal_child_evidence_seed_head_transition"
        ]["disposition"]["recovered_from_checkpoint"] = "forged-checkpoint"
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_disposition_changed",
        ):
            reconcile_required_projection(
                adapter, preview, bad_disposition, remote_head=new_head,
                created_at="2026-08-29T21:01:00Z",
                authenticated_source=source,
                relation_target_resolver=self.relation_target_resolver,
                terminal_child_fence=lambda child_ids: {
                    child_id: expected[child_id] for child_id in child_ids
                },
                projection_input_fence=lambda: prepared[
                    "terminal_child_evidence_seed_head_transition"
                ]["input_frontier_sha256"],
                legacy_unresolved_relation_heads=unresolved,
            )
        self.assertEqual(len(client.comments), writes_before)

        frontier = prepared[
            "terminal_child_evidence_seed_head_transition"
        ]["input_frontier_sha256"]

        def checkpoint_race_fence():
            active = workstream_projection._active_heads(adapter.state())
            disposition = active.get(("disposition", "root"), {}).get("value", {})
            scope_value = active.get(("scope", "root"), {}).get("value", {})
            primary_key = scope_value.get("primary_repository")
            primary = next((
                repository for repository in scope_value.get("repositories", [])
                if repository_key(repository) == primary_key
            ), None)
            if (
                disposition.get("remote_head") == new_head
                and primary is not None
                and primary.get("exact_head") == HEAD
            ):
                return "0" * 64
            return frontier

        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_input_frontier_changed",
        ):
            reconcile_required_projection(
                adapter, preview, prepared, remote_head=new_head,
                created_at="2026-08-29T21:02:00Z",
                authenticated_source=source,
                relation_target_resolver=self.relation_target_resolver,
                terminal_child_fence=lambda child_ids: {
                    child_id: expected[child_id] for child_id in child_ids
                },
                projection_input_fence=checkpoint_race_fence,
                legacy_unresolved_relation_heads=unresolved,
            )
        active_after_race = workstream_projection._active_heads(adapter.state())
        scope_after_race = active_after_race[("scope", "root")]["value"]
        primary_after_race = next(
            repository for repository in scope_after_race["repositories"]
            if repository_key(repository) == scope_after_race["primary_repository"]
        )
        self.assertEqual(primary_after_race["exact_head"], HEAD)

    def terminal_seed_nonprimary_owner_head_transition_fixture(self):
        client, adapter, source, graph, children, manifest, new_head = (
            self.terminal_seed_head_transition_fixture()
        )
        current_scope_event = workstream_projection._active_heads(
            adapter.state()
        )[("scope", "root")]
        current_scope = deepcopy(current_scope_event["value"])
        secondary = deepcopy(current_scope["repositories"][0])
        secondary.update({
            "slug": "github.com/generous-corp/secondary",
            "provider_repository_id": "R_secondary",
        })
        secondary["identity_resolution"].update({
            "provider_repository_id": "R_secondary",
            "resolved_slug": "github.com/generous-corp/secondary",
        })
        secondary["identity_resolution"]["evidence"][0].update({
            "provider_repository_id": "R_secondary",
            "resolved_slug": "github.com/generous-corp/secondary",
        })
        current_scope["repositories"].append(secondary)
        current_scope["child_ownership"]["GEN-70"] = (
            "github.com:id:R_secondary"
        )
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root",
            value=current_scope, plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-29T21:01:00Z",
            supersedes_event_id=current_scope_event["event_id"],
            authority=AUTHORITY,
        ))
        manifest.update(projection_review_contract(adapter.state()))
        desired_scope = deepcopy(current_scope)
        next(
            repository for repository in desired_scope["repositories"]
            if repository_key(repository) == desired_scope["primary_repository"]
        )["exact_head"] = new_head
        next(
            item for item in manifest["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )["value"] = desired_scope
        contract = next(
            item["value"] for item in manifest["projection"]
            if item["kind"] == "evidence_contract"
            and item["value"]["owning_child"] == "GEN-70"
        )
        contract.update({
            "repository": secondary["slug"],
            "repository_key": "github.com:id:R_secondary",
            "exact_head": secondary["exact_head"],
        })
        for layer in contract["layers"].values():
            for receipt in layer.get("receipts", []):
                receipt.update({
                    "repository_key": "github.com:id:R_secondary",
                    "exact_head": secondary["exact_head"],
                })
        transition = manifest["terminal_child_evidence_seed_head_transition"]
        transition.update({
            "from_scope_event_id": adapter.state().events[-1]["event_id"],
            "from_scope_value_sha256": canonical_digest(current_scope),
        })
        return (
            client, adapter, source, graph, children, manifest, new_head,
            deepcopy(secondary),
        )

    def test_terminal_seed_head_transition_refuses_unbound_secondary_owned_seed(self):
        (
            client, adapter, _source, graph, _children, manifest, new_head,
            _secondary,
        ) = self.terminal_seed_nonprimary_owner_head_transition_fixture()
        writes = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_nonprimary_predecessor_required:"
            "GEN-70:gen-70-terminal",
        ):
            prepare_terminal_child_evidence_seeds(
                manifest, graph, adapter.state(), remote_head=new_head,
            )
        self.assertEqual(len(client.comments), writes)

    def test_terminal_seed_head_transition_secondary_owner_negatives(self):
        (
            client, adapter, _source, graph, _children, manifest, new_head,
            secondary,
        ) = self.terminal_seed_nonprimary_owner_head_transition_fixture()
        # A duplicate immutable repository identity in both the reviewed and
        # desired scope must not be collapsed with first-match semantics.
        current_scope_event = workstream_projection._active_heads(
            adapter.state()
        )[("scope", "root")]
        duplicate_current = deepcopy(current_scope_event["value"])
        duplicate_current["repositories"].append(deepcopy(secondary))
        state = adapter.state()
        duplicate_events = deepcopy(list(state.events))
        duplicate_events[-1]["value"] = duplicate_current
        duplicate_state = SimpleNamespace(
            revision=state.revision, events=tuple(duplicate_events),
            snapshot=deepcopy(state.snapshot),
            remote_ids=deepcopy(getattr(state, "remote_ids", {})),
        )
        duplicate = deepcopy(manifest)
        duplicate.update(projection_review_contract(duplicate_state))
        duplicate_scope = next(
            item["value"] for item in duplicate["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        duplicate_scope["repositories"].append(deepcopy(secondary))
        transition = duplicate["terminal_child_evidence_seed_head_transition"]
        transition.update({
            "from_scope_event_id": duplicate_events[-1]["event_id"],
            "from_scope_value_sha256": canonical_digest(duplicate_current),
        })
        duplicate_writes = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_repository_ambiguous:GEN-70",
        ):
            prepare_terminal_child_evidence_seeds(
                duplicate, graph, duplicate_state, remote_head=new_head,
            )
        self.assertEqual(len(client.comments), duplicate_writes)

        (
            mismatch_client, mismatch_adapter, _source, mismatch_graph,
            _children, mismatch, mismatch_head, _secondary,
        ) = self.terminal_seed_nonprimary_owner_head_transition_fixture()
        mismatch = deepcopy(mismatch)
        mismatch_writes = len(mismatch_client.comments)
        contract = next(
            item["value"] for item in mismatch["projection"]
            if item["kind"] == "evidence_contract"
            and item["value"]["owning_child"] == "GEN-70"
        )
        contract["repository_key"] = mismatch["projection"][0]["value"][
            "primary_repository"
        ]
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_contract_invalid:GEN-70",
        ):
            prepare_terminal_child_evidence_seeds(
                mismatch, mismatch_graph, mismatch_adapter.state(),
                remote_head=mismatch_head,
            )
        self.assertEqual(len(mismatch_client.comments), mismatch_writes)

    def closed_children_then_head_transition_fixture(self):
        client, adapter, source, graph, _children, manifest = (
            self.multi_terminal_repair_fixture(evidence_active=True)
        )
        prepared = prepare_terminal_child_repairs(
            manifest, graph, adapter.state(),
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = {
            repair["child_identifier"]: repair["expected_child_readback_sha256"]
            for repair in manifest["terminal_child_repairs"]
        }
        reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-29T21:10:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected[child_id] for child_id in child_ids
            },
            legacy_unresolved_relation_heads=unresolved,
        )
        active = workstream_projection._active_heads(adapter.state())
        projection = [
            {"kind": kind, "key": key, "value": deepcopy(event["value"])}
            for (kind, key), event in sorted(active.items())
            if kind != "disposition"
        ]
        new_head = "b" * 40
        desired_scope = next(
            item["value"] for item in projection if item["kind"] == "scope"
        )
        next(
            repository for repository in desired_scope["repositories"]
            if repository_key(repository) == desired_scope["primary_repository"]
        )["exact_head"] = new_head
        transition = reviewed_manifest(adapter, projection)
        snapshot = add_material_history(
            graph, client.comments, "GEN-37",
            authenticated_route=AUTHORITY, authenticated_source=source,
        )
        reconcile_required_projection(
            adapter, snapshot, transition, remote_head=new_head,
            created_at="2026-08-29T21:11:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
        )
        strict = add_material_history(
            graph, client.comments, "GEN-37",
            authenticated_route=AUTHORITY, authenticated_source=source,
        )
        return client, adapter, source, graph, strict, new_head

    def test_closed_historical_evidence_survives_head_transition_in_resume_and_review(self):
        _client, adapter, _source, _graph, strict, new_head = (
            self.closed_children_then_head_transition_fixture()
        )
        context = compact_context(
            strict, "GEN-37", require_projection_authority=True,
        )
        self.assertEqual(context["resume_authority"], "full")
        self.assertTrue(closure_bound_historical_evidence(
            strict["projection_events"], strict["scope"],
        ))
        result = closure_review(
            strict, expected_plan_revision=PLAN, criteria=[],
            evidence={key: [] for key in (
                "decisions", "followups", "prs", "landing_receipts",
                "tests", "artifacts",
            )},
            required_child_ids={"GEN-70", "GEN-72"},
            choice_events=[], evidence_contracts=strict["evidence_contracts"],
            repository_heads={strict["scope"]["primary_repository"]: new_head},
        )
        self.assertFalse(any(
            error.startswith("evidence_head_mismatch:")
            or error.startswith("closure_history_")
            for error in result["errors"]
        ))

    def test_historical_authority_refuses_late_evidence_and_ownership_drift(self):
        client, adapter, source, graph, strict, new_head = (
            self.closed_children_then_head_transition_fixture()
        )
        late = deepcopy(strict["projection_events"])
        existing = next(
            event for event in late
            if event["kind"] == "evidence_contract"
            and event["value"]["owning_child"] == "GEN-70"
        )
        appended = deepcopy(existing)
        appended["event_id"] = "wsp_" + "f" * 32
        appended["key"] = "gen-70-late"
        appended["value"]["slice_id"] = "gen-70-late"
        late.append(appended)
        with self.assertRaisesRegex(
            ProjectionHistoryError,
            "child_closure_evidence_set_mismatch:history:GEN-70",
        ):
            closure_bound_historical_evidence(late, strict["scope"])

        late_contract = deepcopy(existing["value"])
        late_contract["slice_id"] = "gen-70-add-then-tombstone"
        late_event = build_projection_event(
            workstream_id="GEN-37", kind="evidence_contract",
            key=late_contract["slice_id"], value=late_contract,
            plan_revision=PLAN, expected_revision=adapter.state().revision,
            created_at="2026-08-29T21:14:00Z", authority=AUTHORITY,
        )
        adapter.append(late_event)
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="evidence_contract",
            key=late_contract["slice_id"], value={"_projection_tombstone": True},
            plan_revision=PLAN, expected_revision=adapter.state().revision,
            created_at="2026-08-29T21:15:00Z",
            supersedes_event_id=late_event["event_id"], authority=AUTHORITY,
        ))
        tombstoned = add_material_history(
            graph, client.comments, "GEN-37",
            authenticated_route=AUTHORITY, authenticated_source=source,
        )
        with self.assertRaisesRegex(
            ResumeError,
            "child_closure_evidence_set_mismatch:history:GEN-70",
        ):
            compact_context(
                tombstoned, "GEN-37", require_projection_authority=True,
            )
        review_result = closure_review(
            tombstoned, expected_plan_revision=PLAN, criteria=[],
            evidence={key: [] for key in (
                "decisions", "followups", "prs", "landing_receipts",
                "tests", "artifacts",
            )},
            required_child_ids={"GEN-70", "GEN-72"}, choice_events=[],
            evidence_contracts=tombstoned["evidence_contracts"],
            repository_heads={
                tombstoned["scope"]["primary_repository"]: new_head,
            },
        )
        self.assertIn(
            "child_closure_evidence_set_mismatch:history:GEN-70",
            review_result["errors"],
        )

        drifted_scope = deepcopy(strict["scope"])
        drifted_scope["child_ownership"]["GEN-70"] = "missing-repository"
        with self.assertRaisesRegex(
            ProjectionHistoryError,
            "closure_history_current_scope_invalid:"
            "child_repository_not_participating:GEN-70",
        ):
            closure_bound_historical_evidence(
                strict["projection_events"], drifted_scope,
            )

        malformed_history = deepcopy(strict["projection_events"])
        historical_scope = next(
            event for event in malformed_history
            if (event["kind"], event["key"]) == ("scope", "root")
        )
        historical_scope["value"]["linear"]["route_verification"][
            "workspace_id"
        ] = "wrong-workspace"
        with self.assertRaisesRegex(
            ProjectionHistoryError,
            "closure_history_scope_invalid:GEN-70:"
            "linear_route_readback_mismatch",
        ):
            closure_bound_historical_evidence(
                malformed_history, strict["scope"],
            )

    def test_closure_created_after_transition_cannot_claim_old_head(self):
        _client, _adapter, _source, _graph, strict, _new_head = (
            self.closed_children_then_head_transition_fixture()
        )
        events = deepcopy(strict["projection_events"])
        closure_index = next(
            index for index, event in enumerate(events)
            if (event["kind"], event["key"]) == ("child_closure", "GEN-70")
        )
        scope_index = max(
            index for index, event in enumerate(events)
            if (event["kind"], event["key"]) == ("scope", "root")
        )
        closure_event = events.pop(closure_index)
        if closure_index < scope_index:
            scope_index -= 1
        events.insert(scope_index + 1, closure_event)
        with self.assertRaisesRegex(
            ProjectionHistoryError,
            "closure_history_repository_mismatch:GEN-70",
        ):
            closure_bound_historical_evidence(events, strict["scope"])

    def test_current_head_closure_cannot_precede_its_evidence(self):
        _client, _adapter, _source, _graph, strict, _new_head = (
            self.closed_children_then_head_transition_fixture()
        )
        events = deepcopy(strict["projection_events"])
        closure_index = next(
            index for index, event in enumerate(events)
            if (event["kind"], event["key"]) == ("child_closure", "GEN-70")
        )
        evidence_index = next(
            index for index, event in enumerate(events)
            if event["kind"] == "evidence_contract"
            and event["value"]["owning_child"] == "GEN-70"
        )
        closure_event = events.pop(closure_index)
        if closure_index < evidence_index:
            evidence_index -= 1
        events.insert(evidence_index, closure_event)
        current_scope = deepcopy(strict["scope"])
        next(
            repository for repository in current_scope["repositories"]
            if repository_key(repository) == current_scope["primary_repository"]
        )["exact_head"] = HEAD
        with self.assertRaisesRegex(
            ProjectionHistoryError,
            "closure_history_evidence_set_mismatch:GEN-70",
        ):
            closure_bound_historical_evidence(events, current_scope)

    def test_unclosed_evidence_stays_current_head_bound_across_h1_h2_h3(self):
        client, adapter, source, graph, _strict, h2 = (
            self.closed_children_then_head_transition_fixture()
        )
        contract = evidence_contract()
        contract["slice_id"] = "gen-38-at-h2"
        contract["owning_child"] = "GEN-38"
        contract["exact_head"] = h2
        for layer in contract["layers"].values():
            for receipt in layer.get("receipts", []):
                receipt["exact_head"] = h2
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="evidence_contract",
            key=contract["slice_id"], value=contract, plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-29T21:12:00Z", authority=AUTHORITY,
        ))
        active = workstream_projection._active_heads(adapter.state())
        scope_event = active[("scope", "root")]
        h3_scope = deepcopy(scope_event["value"])
        next(
            repository for repository in h3_scope["repositories"]
            if repository_key(repository) == h3_scope["primary_repository"]
        )["exact_head"] = "c" * 40
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root",
            value=h3_scope, plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-29T21:13:00Z",
            supersedes_event_id=scope_event["event_id"], authority=AUTHORITY,
        ))
        stale = add_material_history(
            graph, client.comments, "GEN-37",
            authenticated_route=AUTHORITY, authenticated_source=source,
        )
        with self.assertRaisesRegex(ResumeError, "evidence_head_mismatch"):
            compact_context(
                stale, "GEN-37", require_projection_authority=True,
            )

    def test_terminal_evidence_seed_recovers_only_canonical_prefix(self):
        client, adapter, _source, graph, _children, manifest = (
            self.multi_terminal_repair_fixture(evidence_active=False)
        )
        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(),
        )
        first = next(
            item for item in prepared["projection"]
            if (item["kind"], item["key"])
            == ("evidence_contract", "gen-70-terminal")
        )
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind=first["kind"], key=first["key"],
            value=first["value"], plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-27T19:00:00Z", authority=AUTHORITY,
        ))
        resumed = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(),
        )
        self.assertEqual(
            resumed["expected_projection_revision"], adapter.state().revision,
        )

        other_client, other_adapter, _, other_graph, _, other_manifest = (
            self.multi_terminal_repair_fixture(evidence_active=False)
        )
        other_prepared = prepare_terminal_child_evidence_seeds(
            other_manifest, other_graph, other_adapter.state(),
        )
        second = next(
            item for item in other_prepared["projection"]
            if (item["kind"], item["key"])
            == ("evidence_contract", "gen-72-terminal")
        )
        other_adapter.append(build_projection_event(
            workstream_id="GEN-37", kind=second["kind"], key=second["key"],
            value=second["value"], plan_revision=PLAN,
            expected_revision=other_adapter.state().revision,
            created_at="2026-08-27T19:00:00Z", authority=AUTHORITY,
        ))
        writes_before = len(other_client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "projection_review_stale_reload_required",
        ):
            prepare_terminal_child_evidence_seeds(
                other_manifest, other_graph, other_adapter.state(),
            )
        self.assertEqual(len(other_client.comments), writes_before)

    def test_terminal_evidence_seed_cannot_change_disposition(self):
        client, adapter, source, graph, _children, manifest = (
            self.multi_terminal_repair_fixture(evidence_active=False)
        )
        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(),
        )
        expected = {
            seed["child_identifier"]: seed["expected_child_readback_sha256"]
            for seed in manifest["terminal_child_evidence_seeds"]
        }
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_evidence_seed_disposition_changed",
        ):
            reconcile_required_projection(
                adapter, graph, prepared, remote_head="b" * 40,
                created_at="2026-08-27T20:00:00Z",
                authenticated_source=source,
                relation_target_resolver=self.relation_target_resolver,
                terminal_child_fence=lambda child_ids: {
                    child_id: expected[child_id] for child_id in child_ids
                },
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_terminal_seed_direct_writer_revalidates_contract_material(self):
        def wrong_owner(contract):
            contract["owning_child"] = "GEN-72"

        def wrong_repository(contract):
            contract["repository_key"] = "github.com:id:R_other"

        def wrong_head(contract):
            contract["exact_head"] = "b" * 40

        def invalid_receipts(contract):
            contract["layers"]["logic"]["receipts"] = []

        for mutate in (
            wrong_owner, wrong_repository, wrong_head, invalid_receipts,
        ):
            with self.subTest(mutate=mutate.__name__):
                client, adapter, source, graph, _children, manifest = (
                    self.multi_terminal_repair_fixture(evidence_active=False)
                )
                item = next(
                    item for item in manifest["projection"]
                    if (item["kind"], item["key"])
                    == ("evidence_contract", "gen-70-terminal")
                )
                mutate(item["value"])
                expected = {
                    seed["child_identifier"]:
                    seed["expected_child_readback_sha256"]
                    for seed in manifest["terminal_child_evidence_seeds"]
                }
                writes_before = len(client.comments)
                with self.assertRaisesRegex(
                    LinearProjectionError,
                    "terminal_child_evidence_seed_contract_invalid",
                ):
                    reconcile_required_projection(
                        adapter, graph, manifest, remote_head=HEAD,
                        created_at="2026-08-27T20:00:00Z",
                        authenticated_source=source,
                        relation_target_resolver=self.relation_target_resolver,
                        terminal_child_fence=lambda child_ids: {
                            child_id: expected[child_id]
                            for child_id in child_ids
                        },
                    )
                self.assertEqual(len(client.comments), writes_before)

    def test_terminal_batch_final_fence_revision_race_refuses(self):
        client, adapter, source, graph, _children, manifest = (
            self.multi_terminal_repair_fixture()
        )
        prepared = prepare_terminal_child_repairs(
            manifest, graph, adapter.state(),
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = {
            repair["child_identifier"]:
            repair["expected_child_readback_sha256"]
            for repair in manifest["terminal_child_repairs"]
        }
        reads = 0

        def racing_fence(child_ids):
            nonlocal reads
            reads += 1
            if reads == 4:
                adapter.append(build_projection_event(
                    workstream_id="GEN-37", kind="provenance", key="late",
                    value={
                        "agent": "other", "machine": "M3",
                        "session_id": "late",
                        "worktree": {"state": "safe", "head": HEAD},
                    },
                    plan_revision=PLAN,
                    expected_revision=adapter.state().revision,
                    created_at="2026-08-27T20:01:00Z", authority=AUTHORITY,
                ))
            return {child_id: expected[child_id] for child_id in child_ids}

        with self.assertRaisesRegex(
            LinearProjectionError, "projection_final_contract_mismatch",
        ):
            reconcile_required_projection(
                adapter, preview, prepared, remote_head=HEAD,
                created_at="2026-08-27T20:00:00Z",
                authenticated_source=source,
                relation_target_resolver=self.relation_target_resolver,
                terminal_child_fence=racing_fence,
                legacy_unresolved_relation_heads=unresolved,
            )

    def test_terminal_seed_partial_prefix_refuses_quarantine_drift(self):
        client, adapter, _source, graph, _children, manifest = (
            self.multi_terminal_repair_fixture(evidence_active=False)
        )
        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(),
        )
        first = next(
            item for item in prepared["projection"]
            if (item["kind"], item["key"])
            == ("evidence_contract", "gen-70-terminal")
        )
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind=first["kind"], key=first["key"],
            value=first["value"], plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-27T19:00:00Z", authority=AUTHORITY,
        ))
        late = legacy_event(
            "provenance", "late-v1", {
                "agent": "other", "machine": "M3", "session_id": "late",
                "worktree": {"state": "safe", "head": HEAD},
            }, 0, "2026-08-27T19:01:00Z",
        )
        client.comments.append(legacy_comment(late, "late-v1-comment"))
        with self.assertRaisesRegex(
            LinearProjectionError, "projection_review_stale_reload_required",
        ):
            prepare_terminal_child_evidence_seeds(
                manifest, graph, adapter.state(),
            )

    def test_terminal_seed_does_not_mask_unrelated_authority_error(self):
        client, adapter, source, graph, _children, manifest = (
            self.multi_terminal_repair_fixture(evidence_active=False)
        )
        choice = record_choice(
            choice_id="invalid-choice", workstream_id="GEN-37",
            owning_child="GEN-999", namespace="agent-workstream-continuity",
            repository="github.com/generous-corp/agent-workstream",
            repository_key="github.com:id:R_agent_workstream",
            plan_revision=PLAN, git_head=HEAD,
            created_at="2026-08-27T19:00:00Z",
            spec_gap="Planted missing owner", decision="Refuse this choice",
            alternatives=["valid owner"], reach="local", irreversible=False,
            domains=[], technical_confidence="high", intent_confidence="high",
        )
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="choice", key=choice["event_id"],
            value=choice, plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-27T19:00:00Z", authority=AUTHORITY,
        ))
        manifest["projection"].append({
            "kind": "choice", "key": choice["event_id"], "value": choice,
        })
        manifest.update(projection_review_contract(adapter.state()))
        prepared = prepare_terminal_child_evidence_seeds(
            manifest, graph, adapter.state(),
        )
        writes_before = len(client.comments)
        with self.assertRaisesRegex(ResumeError, "choice_owner_missing:invalid-choice"):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", prepared, adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_multi_terminal_repair_recovers_only_from_ordered_prefix(self):
        client, adapter, source, graph, _children, stale_manifest = (
            self.multi_terminal_repair_fixture()
        )
        prepared = prepare_terminal_child_repairs(
            stale_manifest, graph, adapter.state(),
        )
        closure_items = {
            item["key"]: item for item in prepared["projection"]
            if item["kind"] == "child_closure"
        }
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="child_closure", key="GEN-70",
            value=closure_items["GEN-70"]["value"], plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-27T19:00:00Z", authority=AUTHORITY,
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

        resumed = prepare_terminal_child_repairs(
            stale_manifest, graph, adapter.state(),
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", resumed, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = {
            repair["child_identifier"]: repair[
                "expected_child_readback_sha256"
            ]
            for repair in stale_manifest["terminal_child_repairs"]
        }
        partial_revision = adapter.state().revision
        reconcile_required_projection(
            adapter, preview, resumed, remote_head=HEAD,
            created_at="2026-08-27T20:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda child_ids: {
                child_id: expected[child_id] for child_id in child_ids
            },
            legacy_unresolved_relation_heads=unresolved,
        )
        self.assertEqual(
            [(event["kind"], event["key"])
             for event in adapter.state().events[partial_revision:]],
            [("child_closure", "GEN-72")],
        )

        other_client, other_adapter, _source, other_graph, _, other_manifest = (
            self.multi_terminal_repair_fixture()
        )
        other_prepared = prepare_terminal_child_repairs(
            other_manifest, other_graph, other_adapter.state(),
        )
        gen72 = next(
            item for item in other_prepared["projection"]
            if (item["kind"], item["key"]) == ("child_closure", "GEN-72")
        )
        other_adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="child_closure", key="GEN-72",
            value=gen72["value"], plan_revision=PLAN,
            expected_revision=other_adapter.state().revision,
            created_at="2026-08-27T19:00:00Z", authority=AUTHORITY,
        ))
        writes_before = len(other_client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError, "projection_review_stale_reload_required",
        ):
            prepare_terminal_child_repairs(
                other_manifest, other_graph, other_adapter.state(),
            )
        self.assertEqual(len(other_client.comments), writes_before)

    def test_multi_terminal_repair_fences_every_child_from_one_snapshot(self):
        client, adapter, source, graph, _children, stale_manifest = (
            self.multi_terminal_repair_fixture()
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
        expected = {
            repair["child_identifier"]: repair[
                "expected_child_readback_sha256"
            ]
            for repair in stale_manifest["terminal_child_repairs"]
        }
        fence_reads = 0

        def changing_batch_fence(child_ids):
            nonlocal fence_reads
            fence_reads += 1
            values = {child_id: expected[child_id] for child_id in child_ids}
            if fence_reads >= 3:
                values["GEN-72"] = "0" * 64
            return values

        before_revision = adapter.state().revision
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_readback_changed_reload_required:GEN-72",
        ):
            reconcile_required_projection(
                adapter, preview, prepared, remote_head=HEAD,
                created_at="2026-08-27T20:00:00Z",
                authenticated_source=source,
                relation_target_resolver=self.relation_target_resolver,
                terminal_child_fence=changing_batch_fence,
                legacy_unresolved_relation_heads=unresolved,
            )
        self.assertEqual(
            [(event["kind"], event["key"])
             for event in adapter.state().events[before_revision:]],
            [("child_closure", "GEN-70")],
        )
        self.assertNotIn("GEN-72", adapter.state().snapshot.get("child_closures", {}))

    def test_multi_terminal_direct_writer_requires_complete_repair_set(self):
        client, adapter, source, graph, _children, manifest = (
            self.multi_terminal_repair_fixture()
        )
        manifest["terminal_child_repairs"] = manifest[
            "terminal_child_repairs"
        ][:1]
        prepared = prepare_terminal_child_repairs(
            manifest, graph, adapter.state(),
        )
        writes_before = len(client.comments)
        expected = prepared["terminal_child_repairs"][0][
            "expected_child_readback_sha256"
        ]
        with self.assertRaisesRegex(
            LinearProjectionError, "terminal_child_repairs_incomplete:GEN-72",
        ):
            reconcile_required_projection(
                adapter, graph, prepared, remote_head=HEAD,
                created_at="2026-08-27T20:00:00Z",
                authenticated_source=source,
                relation_target_resolver=self.relation_target_resolver,
                terminal_child_fence=fixed_terminal_fence(expected),
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_multi_terminal_repair_requires_complete_batch_fence(self):
        for observed_keys in ({"GEN-70"}, {"GEN-70", "GEN-72", "GEN-99"}):
            with self.subTest(observed_keys=observed_keys):
                client, adapter, source, graph, _children, manifest = (
                    self.multi_terminal_repair_fixture()
                )
                prepared = prepare_terminal_child_repairs(
                    manifest, graph, adapter.state(),
                )
                preview, unresolved = (
                    load_material_history_for_projection_reconcile(
                        graph, client.comments, "GEN-37", prepared, adapter,
                        authenticated_route=AUTHORITY,
                        authenticated_source=source, remote_head=HEAD,
                        relation_target_resolver=self.relation_target_resolver,
                    )
                )
                expected = {
                    repair["child_identifier"]: repair[
                        "expected_child_readback_sha256"
                    ]
                    for repair in manifest["terminal_child_repairs"]
                }
                writes_before = len(client.comments)
                with self.assertRaisesRegex(
                    LinearProjectionError,
                    "terminal_child_readback_fence_incomplete_reload_required",
                ):
                    reconcile_required_projection(
                        adapter, preview, prepared, remote_head=HEAD,
                        created_at="2026-08-27T20:00:00Z",
                        authenticated_source=source,
                        relation_target_resolver=self.relation_target_resolver,
                        terminal_child_fence=lambda _child_ids: {
                            child_id: expected.get(child_id, "0" * 64)
                            for child_id in observed_keys
                        },
                        legacy_unresolved_relation_heads=unresolved,
                    )
                self.assertEqual(len(client.comments), writes_before)

    def test_multi_terminal_repair_manifest_ambiguity_refuses_without_writes(self):
        for mutation, expected in (
            (lambda repairs: repairs.reverse(),
             "terminal_child_repairs_not_canonical"),
            (lambda repairs: repairs.append(deepcopy(repairs[0])),
             "duplicate_manifest_terminal_child_repair"),
            (lambda repairs: repairs[1].__setitem__(
                "approved_evidence_heads",
                deepcopy(repairs[0]["approved_evidence_heads"]),
             ), "overlapping_manifest_terminal_child_evidence"),
        ):
            with self.subTest(expected=expected):
                client, adapter, _source, graph, _children, manifest = (
                    self.multi_terminal_repair_fixture()
                )
                mutation(manifest["terminal_child_repairs"])
                writes_before = len(client.comments)
                with self.assertRaisesRegex(LinearProjectionError, expected):
                    prepare_terminal_child_repairs(
                        manifest, graph, adapter.state(),
                    )
                self.assertEqual(len(client.comments), writes_before)

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
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            RelationReadbackError, "relation_target_readback_incomplete",
        ):
            load_material_history_for_projection_reconcile(
                self.graph_snapshot(), client.comments, "GEN-37", manifest,
                adapter, authenticated_route=AUTHORITY,
                authenticated_source=source, remote_head=HEAD,
                relation_target_resolver=self.incomplete_relation_target_resolver,
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
            terminal_child_fence=fixed_terminal_fence(
                repair["expected_child_readback_sha256"]
            ),
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
            terminal_child_fence=fixed_terminal_fence(
                repair["expected_child_readback_sha256"]
            ),
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
        cases = [
            ("terminal_child_readback_changed_reload_required", lambda graph, manifest: (
                graph["children"][1].__setitem__("assignee", None)
            )),
            ("terminal_child_not_completed", lambda graph, manifest: (
                graph["children"][1].__setitem__("status_type", "canceled")
            )),
            ("terminal_child_repair_route_mismatch:project_id", lambda graph, manifest: (
                graph["children"][1].__setitem__("project", {"id": "other-project"}),
                manifest["terminal_child_repairs"][0].__setitem__(
                    "expected_child_readback_sha256",
                    canonical_digest(terminal_child_readback(graph["children"][1])),
                ),
            )),
            ("terminal_child_readback_changed_reload_required", lambda graph, manifest: (
                manifest["terminal_child_repairs"][0].__setitem__(
                    "expected_assignee_id", "other-assignee"
                )
            )),
            ("terminal_child_repair_evidence_set_changed_reload_required", lambda graph, manifest: (
                manifest["terminal_child_repairs"][0]["approved_evidence_heads"][0].__setitem__(
                    "value_sha256", "0" * 64
                )
            )),
        ]
        for expected, mutate in cases:
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
            terminal_child_fence=fixed_terminal_fence(
                stale_manifest["terminal_child_repairs"][0][
                    "expected_child_readback_sha256"
                ]
            ),
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
                terminal_child_fence=fixed_terminal_fence("0" * 64),
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

        def changing_fence(child_ids):
            nonlocal reads
            reads += 1
            value = expected if reads <= 2 else "0" * 64
            return {child_id: value for child_id in child_ids}

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
                    terminal_child_fence=fixed_terminal_fence(expected_digest),
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
            terminal_child_fence=fixed_terminal_fence(expected),
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
            LinearProjectionError,
            "terminal_child_closure_retirement_forbidden:GEN-72",
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
            LinearProjectionError,
            "terminal_child_closure_retirement_forbidden:GEN-72",
        ):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", paired_manifest, adapter,
                authenticated_route=AUTHORITY,
                authenticated_source=source, remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_closure_retirement_forbidden:GEN-72",
        ):
            reconcile_required_projection(
                adapter, graph, paired_manifest, remote_head=HEAD,
                created_at="2026-08-27T20:10:00Z",
                authenticated_source=source,
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
            LinearProjectionError,
            "terminal_child_closure_retirement_forbidden:GEN-72",
        ):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", reassigned_manifest, adapter,
                authenticated_route=AUTHORITY,
                authenticated_source=source, remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_closure_retirement_forbidden:GEN-72",
        ):
            reconcile_required_projection(
                adapter, graph, reassigned_manifest, remote_head=HEAD,
                created_at="2026-08-27T20:20:00Z",
                authenticated_source=source,
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
            terminal_child_fence=fixed_terminal_fence(expected),
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
        forged_with_repair = {
            **replacement_manifest,
            "terminal_child_repairs": deepcopy(
                stale_manifest["terminal_child_repairs"]
            ),
        }
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_closure_readback_digest_mismatch:GEN-72",
        ):
            reconcile_required_projection(
                adapter, graph, forged_with_repair, remote_head=HEAD,
                created_at="2026-08-27T20:16:00Z",
                authenticated_source=source,
                terminal_child_fence=fixed_terminal_fence(expected),
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
        combined_projection = deepcopy(prepared["projection"])
        combined_evidence = next(
            item for item in combined_projection
            if item["kind"] == "evidence_contract"
            and item["value"].get("owning_child") == "GEN-72"
        )
        combined_evidence["value"]["layers"]["logic"]["receipts"][0][
            "id"
        ] = "simultaneous-replacement"
        combined_manifest = {
            **reviewed_manifest(adapter, combined_projection),
            "terminal_child_repairs": deepcopy(
                stale_manifest["terminal_child_repairs"]
            ),
        }
        with self.assertRaisesRegex(
            ResumeError, "child_closure_evidence_set_mismatch",
        ):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", combined_manifest, adapter,
                authenticated_route=AUTHORITY,
                authenticated_source=source, remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_repair_unrelated_change:evidence_contract:",
        ):
            reconcile_required_projection(
                adapter, graph, combined_manifest, remote_head=HEAD,
                created_at="2026-08-27T20:50:00Z",
                authenticated_source=source,
                terminal_child_fence=fixed_terminal_fence(expected),
            )
        self.assertEqual(len(client.comments), writes_before)
        scope_mutations = {
            "namespace": lambda value: value.__setitem__(
                "namespace", "forged-namespace"
            ),
            "unrelated_owner": lambda value: value["child_ownership"].__setitem__(
                "GEN-999", "github.com:id:R_agent_workstream"
            ),
            "repository_alias": lambda value: value["repositories"][0][
                "aliases"
            ].append("github.com/forged/alias"),
        }
        for name, mutate_scope in scope_mutations.items():
            with self.subTest(scope_mutation=name):
                widened_projection = deepcopy(prepared["projection"])
                widened_scope = next(
                    item["value"] for item in widened_projection
                    if item["kind"] == "scope"
                )
                mutate_scope(widened_scope)
                widened_manifest = {
                    **reviewed_manifest(adapter, widened_projection),
                    "terminal_child_repairs": deepcopy(
                        stale_manifest["terminal_child_repairs"]
                    ),
                }
                with self.assertRaisesRegex(
                    LinearProjectionError,
                    "terminal_child_repair_scope_widened",
                ):
                    reconcile_required_projection(
                        adapter, graph, widened_manifest, remote_head=HEAD,
                        created_at="2026-08-27T20:55:00Z",
                        authenticated_source=source,
                        terminal_child_fence=fixed_terminal_fence(expected),
                    )
                self.assertEqual(len(client.comments), writes_before)
        self.assertEqual(closure_item["value"]["child_identifier"], "GEN-72")

    def test_legacy_unresolved_relation_retirement_precedes_unrelated_writes(self):
        client, adapter, base, source = self.legacy_relation_fixture()
        retirement = reviewed_retirement(adapter, "relation", "blocks:GEN-14")
        desired = [*base[:-2], {
            "kind": "provenance", "key": "old", "value": {
                "agent": "claude", "machine": "M3", "session_id": "new",
                "worktree": {"state": "safe", "head": HEAD},
            },
        }]
        manifest = reviewed_manifest(adapter, desired, [retirement])
        graph = self.graph_snapshot()
        graph["children"] = [{
            "identifier": "GEN-38", "title": "Owned child",
            "url": "https://linear.app/acme/issue/GEN-38/child",
            "status": "In Progress", "next_action": "Continue.",
        }]
        snapshot, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", manifest, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
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

    def test_legacy_first_backfill_requires_exact_later_identity_seal(self):
        initial_value = scope()
        backfilled_value = deepcopy(initial_value)
        repository = backfilled_value["repositories"][0]
        old_slug = "github.com/danielraffel/agent-workstream"
        observed_at = "2026-08-28T03:35:30Z"
        repository["aliases"] = [old_slug]
        repository["identity_resolution"]["observed_at"] = observed_at
        repository["identity_updates"] = [{
            "from": old_slug,
            "to": repository["slug"],
            "repository_key": "github.com:id:R_agent_workstream",
            "provider_repository_id": "R_agent_workstream",
            "observed_at": observed_at,
            "evidence": [{
                "kind": "authenticated_provider_readback", "authenticated": True,
                "repository_key": "github.com:id:R_agent_workstream",
                "provider_repository_id": "R_agent_workstream",
                "requested_slug": old_slug,
                "resolved_slug": repository["slug"],
            }],
        }]
        first = self.event("scope", "root", initial_value, 0)
        source = self.event(
            "source", "root", {"identity": "plan:test", "sha256": PLAN}, 1,
        )
        second = self.event(
            "scope", "root", backfilled_value, 2, supersedes=first["event_id"],
        )
        first_comment = {
            **projection_comment(first),
            "createdAt": "2026-08-28T03:34:00.000Z",
            "updatedAt": "2026-08-28T03:34:00.000Z",
        }
        second_comment = {
            **projection_comment(second),
            "createdAt": "2026-08-28T03:36:53.292Z",
            "updatedAt": "2026-08-28T03:36:53.273Z",
        }
        source_comment = {
            **projection_comment(source),
            "createdAt": "2026-08-28T03:35:00.000Z",
            "updatedAt": "2026-08-28T03:35:00.000Z",
        }
        with self.assertRaisesRegex(
            LinearProjectionError, "repository_identity_history_regressed",
        ):
            reduce_projection_comments(
                [first_comment, source_comment, second_comment], workstream_id="GEN-37",
                expected_plan_revision=PLAN,
            )
        inspection = projection_module.inspect_unsealed_identity_history(
            [first_comment, source_comment, second_comment], workstream_id="GEN-37",
            expected_plan_revision=PLAN, authenticated_route=AUTHORITY,
            authenticated_source={"identity": "plan:test", "sha256": PLAN},
            material_revision=0,
        )
        self.assertEqual(inspection["resume_authority"], "none")
        self.assertNotIn("scope", inspection)
        self.assertEqual(
            inspection["candidate"]["sealed_scope_event_id"], second["event_id"],
        )
        proof = [{
            "repository_key": "github.com:id:R_agent_workstream",
            "provider_repository_id": "R_agent_workstream",
            "canonical_slug": repository["slug"],
            "routes": [
                {
                    "requested_slug": route,
                    "resolved_slug": repository["slug"],
                    "provider_repository_id": "R_agent_workstream",
                    "requested_response_url": f"https://api.github.test/{route}",
                    "canonical_response_url": "https://api.github.test/canonical",
                    "redirect_count": 0 if route == repository["slug"] else 1,
                    "authenticated": True,
                }
                for route in sorted([repository["slug"], old_slug])
            ],
        }]
        receipt_comments = {
            first["event_id"]: first_comment, source["event_id"]: source_comment,
            second["event_id"]: second_comment,
        }
        seal_value = {
            "schema_version": 1,
            "root_issue_id": ROOT_UUID,
            "plan_revision": PLAN,
            "source_identity": "plan:test",
            "source_sha256": PLAN,
            "expected_material_revision": 0,
            "expected_projection_revision": 3,
            "sealed_scope_event_id": second["event_id"],
            "sealed_scope_value_sha256": hashlib.sha256(
                projection_module._canonical(backfilled_value)
            ).hexdigest(),
            "legacy_transitions": [{
                "predecessor_scope_event_id": first["event_id"],
                "predecessor_scope_value_sha256": hashlib.sha256(
                    projection_module._canonical(initial_value)
                ).hexdigest(),
                "transition_scope_event_id": second["event_id"],
                "transition_scope_value_sha256": hashlib.sha256(
                    projection_module._canonical(backfilled_value)
                ).hexdigest(),
            }],
            "sealed_projection_frontier_event_id": second["event_id"],
            "sealed_projection_frontier_event_sha256": hashlib.sha256(
                projection_module._canonical(second)
            ).hexdigest(),
            "legacy_projection_prefix_sha256": projection_module.projection_prefix_sha256(
                [first, source, second], receipt_comments, second["event_id"],
            ),
            "repositories": proof,
            "repositories_sha256": hashlib.sha256(
                projection_module._canonical(proof)
            ).hexdigest(),
            "observed_at": "2026-08-29T12:00:00Z",
        }
        seal = self.event(
            "identity_history_seal", second["event_id"], seal_value, 3,
        )
        seal_comment = {
            **projection_comment(seal),
            "createdAt": "2026-08-29T12:00:00.000Z",
            "updatedAt": "2026-08-29T12:00:00.000Z",
        }
        reduced = reduce_projection_comments(
            [first_comment, source_comment, second_comment, seal_comment], workstream_id="GEN-37",
            expected_plan_revision=PLAN,
        )
        self.assertEqual(reduced.snapshot["scope"], backfilled_value)

        twice_backfilled = deepcopy(backfilled_value)
        second_old_slug = "github.com/example/agent-workstream"
        twice_repository = twice_backfilled["repositories"][0]
        twice_repository["aliases"].append(second_old_slug)
        twice_repository["identity_resolution"]["observed_at"] = (
            "2026-08-28T04:35:30Z"
        )
        twice_repository["identity_updates"].append({
            "from": second_old_slug, "to": twice_repository["slug"],
            "repository_key": "github.com:id:R_agent_workstream",
            "provider_repository_id": "R_agent_workstream",
            "observed_at": "2026-08-28T04:35:30Z",
            "evidence": [{
                "kind": "authenticated_provider_readback", "authenticated": True,
                "repository_key": "github.com:id:R_agent_workstream",
                "provider_repository_id": "R_agent_workstream",
                "requested_slug": second_old_slug,
                "resolved_slug": twice_repository["slug"],
            }],
        })
        third = self.event(
            "scope", "root", twice_backfilled, 3,
            supersedes=second["event_id"],
        )
        third_comment = {
            **projection_comment(third),
            "createdAt": "2026-08-28T04:36:53.292Z",
            "updatedAt": "2026-08-28T04:36:53.273Z",
        }
        twice_inspection = projection_module.inspect_unsealed_identity_history(
            [first_comment, source_comment, second_comment, third_comment],
            workstream_id="GEN-37", expected_plan_revision=PLAN,
            authenticated_route=AUTHORITY,
            authenticated_source={"identity": "plan:test", "sha256": PLAN},
            material_revision=0,
        )
        self.assertEqual(
            [item["transition_scope_event_id"] for item in
             twice_inspection["candidate"]["legacy_transitions"]],
            [second["event_id"], third["event_id"]],
        )
        twice_proof = deepcopy(proof)
        twice_proof[0]["routes"].append({
            "requested_slug": second_old_slug,
            "resolved_slug": twice_repository["slug"],
            "provider_repository_id": "R_agent_workstream",
            "requested_response_url": f"https://api.github.test/{second_old_slug}",
            "canonical_response_url": "https://api.github.test/canonical",
            "redirect_count": 1, "authenticated": True,
        })
        twice_proof[0]["routes"].sort(key=lambda item: item["requested_slug"])
        twice_seal_value = {
            **seal_value,
            **twice_inspection["candidate"],
            "expected_projection_revision": 4,
            "repositories": twice_proof,
            "repositories_sha256": hashlib.sha256(
                projection_module._canonical(twice_proof)
            ).hexdigest(),
        }
        planted_value = {
            **twice_seal_value,
            "sealed_scope_event_id": second["event_id"],
            "sealed_scope_value_sha256": hashlib.sha256(
                projection_module._canonical(backfilled_value)
            ).hexdigest(),
            "repositories": proof,
            "repositories_sha256": hashlib.sha256(
                projection_module._canonical(proof)
            ).hexdigest(),
        }
        planted = self.event(
            "identity_history_seal", second["event_id"], planted_value, 4,
        )
        planted_comment = {
            **projection_comment(planted),
            "createdAt": "2026-08-29T12:01:30.000Z",
            "updatedAt": "2026-08-29T12:01:30.000Z",
        }
        with self.assertRaisesRegex(
            LinearProjectionError, "identity_history_seal_frontier_mismatch",
        ):
            reduce_projection_comments(
                [first_comment, source_comment, second_comment, third_comment,
                 planted_comment],
                workstream_id="GEN-37", expected_plan_revision=PLAN,
                authenticated_route=AUTHORITY,
                authenticated_source={"identity": "plan:test", "sha256": PLAN},
            )
        incomplete_value = deepcopy(twice_seal_value)
        incomplete_value["legacy_transitions"] = incomplete_value[
            "legacy_transitions"
        ][:1]
        incomplete = self.event(
            "identity_history_seal", third["event_id"], incomplete_value, 4,
        )
        incomplete_comment = {
            **projection_comment(incomplete),
            "createdAt": "2026-08-29T12:01:45.000Z",
            "updatedAt": "2026-08-29T12:01:45.000Z",
        }
        with self.assertRaisesRegex(
            LinearProjectionError, "identity_history_seal_transition_mismatch",
        ):
            reduce_projection_comments(
                [first_comment, source_comment, second_comment, third_comment,
                 incomplete_comment],
                workstream_id="GEN-37", expected_plan_revision=PLAN,
                authenticated_route=AUTHORITY,
                authenticated_source={"identity": "plan:test", "sha256": PLAN},
            )
        twice_seal = self.event(
            "identity_history_seal", third["event_id"], twice_seal_value, 4,
        )
        twice_seal_comment = {
            **projection_comment(twice_seal),
            "createdAt": "2026-08-29T12:02:00.000Z",
            "updatedAt": "2026-08-29T12:02:00.000Z",
        }
        twice_reduced = reduce_projection_comments(
            [first_comment, source_comment, second_comment, third_comment,
             twice_seal_comment],
            workstream_id="GEN-37", expected_plan_revision=PLAN,
            authenticated_route=AUTHORITY,
            authenticated_source={"identity": "plan:test", "sha256": PLAN},
        )
        self.assertEqual(twice_reduced.snapshot["scope"], twice_backfilled)

        forged_value = deepcopy(backfilled_value)
        forged_repository = forged_value["repositories"][0]
        forged_slug = "github.com/attacker/forged"
        forged_repository["aliases"] = [forged_slug]
        forged_repository["identity_updates"][0]["from"] = forged_slug
        forged_repository["identity_updates"][0]["evidence"][0][
            "requested_slug"
        ] = forged_slug
        forged = self.event(
            "scope", "root", forged_value, 2, supersedes=first["event_id"],
        )
        forged_comment = {**second_comment, "body": encode_projection_comment(forged)}
        with self.assertRaisesRegex(
            LinearProjectionError, "identity_history_seal_frontier_mismatch",
        ):
            reduce_projection_comments(
                [first_comment, source_comment, forged_comment, seal_comment], workstream_id="GEN-37",
                expected_plan_revision=PLAN,
            )

        def changed_seal(**changes):
            changed = deepcopy(seal_value)
            changed.update(changes)
            if "repositories" in changes:
                changed["repositories_sha256"] = hashlib.sha256(
                    projection_module._canonical(changed["repositories"])
                ).hexdigest()
            event = self.event(
                "identity_history_seal", second["event_id"], changed, 3,
            )
            return {
                **projection_comment(event),
                "createdAt": seal_comment["createdAt"],
                "updatedAt": seal_comment["updatedAt"],
            }

        with self.assertRaisesRegex(
            LinearProjectionError, "identity_history_seal_frontier_mismatch",
        ):
            reduce_projection_comments(
                [first_comment, source_comment, second_comment, changed_seal(
                    legacy_projection_prefix_sha256="0" * 64,
                )],
                workstream_id="GEN-37", expected_plan_revision=PLAN,
            )

        wrong_proof = deepcopy(proof)
        wrong_proof[0]["provider_repository_id"] = "R_attacker"
        for route in wrong_proof[0]["routes"]:
            route["provider_repository_id"] = "R_attacker"
        with self.assertRaisesRegex(
            LinearProjectionError, "identity_history_seal_repository_mismatch",
        ):
            reduce_projection_comments(
                [first_comment, source_comment, second_comment,
                 changed_seal(repositories=wrong_proof)],
                workstream_id="GEN-37", expected_plan_revision=PLAN,
            )

        repeated_seal_value = deepcopy(seal_value)
        repeated_seal_value["expected_projection_revision"] = 4
        second_seal = self.event(
            "identity_history_seal", second["event_id"], repeated_seal_value, 4,
            supersedes=seal["event_id"],
        )
        second_seal_comment = {
            **projection_comment(second_seal),
            "createdAt": "2026-08-29T12:01:00.000Z",
            "updatedAt": "2026-08-29T12:01:00.000Z",
        }
        with self.assertRaisesRegex(
            LinearProjectionError, "identity_history_seal_frontier_mismatch",
        ):
            reduce_projection_comments(
                [first_comment, source_comment, second_comment, seal_comment,
                 second_seal_comment],
                workstream_id="GEN-37", expected_plan_revision=PLAN,
            )

        seal_shaped_plain_comment = {
            "id": "not-a-projection-slot", "body": json.dumps(seal_value),
            "createdAt": seal_comment["createdAt"],
            "updatedAt": seal_comment["updatedAt"],
        }
        with self.assertRaisesRegex(
            LinearProjectionError, "repository_identity_history_regressed",
        ):
            reduce_projection_comments(
                [first_comment, source_comment, second_comment,
                 seal_shaped_plain_comment],
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
        self.assertIsNone(context["provenance"]["latest"])
        self.assertIsNone(context["provenance"]["latest_projection_head"])
        self.assertEqual(context["provenance"]["count"], 1)
        self.assertRegex(context["provenance"]["sha256"], r"^[0-9a-f]{64}$")
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

    def generation_checkpoint_ownership_repair_fixture(self):
        predecessor_plan = "b" * 64
        client = FakeProjectionClient()

        def projection_adapter(plan_revision):
            return LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=plan_revision, **AUTHORITY,
            )

        def project_generation(plan_revision):
            target = projection_adapter(plan_revision)
            generation_scope = scope()
            generation_scope["child_ownership"] = {}
            source = {
                "identity": f"https://example.test/{plan_revision}",
                "sha256": plan_revision,
            }
            values = [
                ("scope", "root", generation_scope),
                ("source", "root", source),
                ("provenance", "generation", {
                    "agent": "codex", "machine": "M5",
                    "session_id": plan_revision[:8],
                    "worktree": {"state": "safe", "head": HEAD},
                }),
                ("disposition", "root", {
                    "disposition": "attach", "remote_head": HEAD,
                    "recovered_from_checkpoint": None,
                }),
            ]
            for revision, (kind, key, value) in enumerate(values):
                target.append(build_projection_event(
                    workstream_id="GEN-37", kind=kind, key=key,
                    value=value, plan_revision=plan_revision,
                    expected_revision=revision, created_at=str(revision),
                    authority=AUTHORITY,
                ))
            return source

        project_generation(predecessor_plan)
        source = project_generation(PLAN)
        target = projection_adapter(PLAN)
        while target.state().revision < 16:
            revision = target.state().revision
            target.append(build_projection_event(
                workstream_id="GEN-37", kind="provenance",
                key=f"target-padding-{revision}", value={
                    "agent": "codex", "machine": "M5",
                    "session_id": f"target-padding-{revision}",
                    "worktree": {"state": "safe", "head": HEAD},
                }, plan_revision=PLAN, expected_revision=revision,
                created_at=f"target-{revision}", authority=AUTHORITY,
            ))
        activation_checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="activate-target",
            root_revision=0, plan_revision=PLAN,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "activate-target", "machine": "M5",
                "worktree": {
                    "state": "safe", "path": "/tmp/target",
                    "branch": "target", "head": HEAD,
                },
            }, exact_head=HEAD, evidence=[], blocker=None,
            next_action="Continue the activated target generation.",
        )

        def candidate_loader(plan_revision):
            state = projection_adapter(plan_revision).state()
            checkpoints = reduce_generation_checkpoint_comments(
                client.comments, workstream_id="GEN-37",
                authenticated_route=AUTHORITY,
            )
            checkpoint_ids = sorted(
                checkpoint["event_id"]
                for checkpoint in checkpoints.checkpoints
                if checkpoint["plan_revision"] == plan_revision
            )
            if plan_revision == PLAN:
                checkpoint_ids = sorted({
                    *checkpoint_ids, activation_checkpoint["event_id"],
                })
            material = reduce_event_comments(
                client.comments, workstream_id="GEN-37",
            )
            return {
                "resume_authority": "full",
                "plan_revision": plan_revision,
                "authenticated_route": AUTHORITY,
                "source": state.snapshot["source"],
                "material_revision": material.revision,
                "checkpoint_event_ids": checkpoint_ids,
                "projection_revision": state.revision,
                "graph_frontier_sha256": generation_digest("stable-graph"),
                "snapshot_sha256": generation_digest({
                    "projection_event_ids": [
                        event["event_id"] for event in state.events
                    ],
                    "checkpoint_event_ids": checkpoint_ids,
                }),
                "quarantined_legacy_writes": generation_quarantine_metadata(
                    client.comments, workstream_id="GEN-37",
                ),
            }

        predecessor = projection_adapter(predecessor_plan).state()
        generation = GenerationTransport(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            authority=AUTHORITY, candidate_loader=candidate_loader,
            legacy_description_plan_revision=predecessor_plan,
        )
        generation._capability_checked = True
        generation.activate(
            target_plan_revision=PLAN, created_at="activate",
            retirement=build_retirement_proof(
                predecessor_plan_revision=predecessor_plan,
                retired_at="activate", retired_writer_epoch=0,
                provenance_event_ids=[
                    event["event_id"] for event in predecessor.events
                    if event["kind"] == "provenance"
                ], checkpoint_event_ids=[],
            ),
            activation_checkpoint=activation_checkpoint, remote_head=HEAD,
        )
        self.assertEqual(target.state().revision, 18)
        selected = select_plan_generation(
            client.comments, workstream_id="GEN-37",
            description_plan_revision=predecessor_plan,
            authenticated_route=AUTHORITY,
        )
        transition = next(
            event for event in projection_adapter(predecessor_plan).state().events
            if event["event_id"] == selected["transition_tip_event_id"]
        )
        self.assertEqual(transition["value"]["schema_version"], 3)

        for child_number in range(91, 95):
            revision = target.state().revision
            child_issue_id = f"child-uuid-{child_number}"
            target.append(build_projection_event(
                workstream_id="GEN-37",
                kind="child_extension_authorization", key=child_issue_id,
                value={
                    "root_issue_id": ROOT_UUID, "route": AUTHORITY,
                    "source": source, "plan_revision": PLAN,
                    "reviewed_candidate_key": f"child-{child_number}",
                    "child_issue_id": child_issue_id,
                    "expected_material_revision": 0,
                    "expected_projection_revision": revision,
                    "initial_state": "planned_pending_projection",
                }, plan_revision=PLAN, expected_revision=revision,
                created_at=f"child-{child_number}", authority=AUTHORITY,
            ))
        state = target.state()
        self.assertEqual(state.revision, 22)
        desired_scope = deepcopy(state.snapshot["scope"])
        desired_scope["child_ownership"].update({
            f"GEN-{child_number}": "github.com:id:R_agent_workstream"
            for child_number in range(91, 95)
        })
        manifest = {
            **projection_review_contract(state), "retirements": [],
            "projection": [
                {"kind": "scope", "key": "root", "value": desired_scope},
                {"kind": "source", "key": "root", "value": source},
                {"kind": "provenance", "key": "generation", "value": (
                    state.snapshot["provenance"][0]
                )},
            ],
        }
        graph = {
            "root": {
                "identifier": "GEN-37", "url": "https://linear/GEN-37",
                "description_plan_revision": predecessor_plan,
                "plan_revision": PLAN, "revision": 0,
                "status": "In Progress", "next_action": "continue",
                "generation_transition_tip_event_id": selected[
                    "transition_tip_event_id"
                ],
                "generation_activation_epoch": selected["activation_epoch"],
                "generation_authority_origin": selected["authority_origin"],
            },
            "children": [{
                "identifier": f"GEN-{child_number}",
                "title": f"Child {child_number}",
                "url": f"https://linear/GEN-{child_number}",
                "status": "In Progress", "status_type": "started",
                "next_action": "continue",
            } for child_number in range(91, 95)],
            "decisions": [],
        }
        snapshot, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", manifest, target,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD, relation_target_resolver=lambda _relations: {},
        )
        return {
            "client": client, "target": target, "source": source,
            "manifest": manifest, "graph": graph, "snapshot": snapshot,
            "unresolved": unresolved, "checkpoint": activation_checkpoint,
        }

    def test_generation_activation_checkpoint_fences_ownership_only_repair(self):
        fixture = self.generation_checkpoint_ownership_repair_fixture()
        checkpoint_id = fixture["checkpoint"]["event_id"]
        self.assertEqual(
            fixture["snapshot"]["latest_checkpoint"]["checkpoint_event_id"],
            checkpoint_id,
        )

        def checkpoint_fence():
            return workstream_projection.latest_acknowledged_checkpoint_id_from_comments(
                fixture["client"].comments, workstream_id="GEN-37",
                plan_revision=PLAN, authenticated_route=AUTHORITY,
            )

        self.assertEqual(checkpoint_fence(), checkpoint_id)
        self.assertIsNone(
            workstream_projection.latest_acknowledged_checkpoint_id_from_comments(
                fixture["client"].comments, workstream_id="GEN-37",
                plan_revision="b" * 64, authenticated_route=AUTHORITY,
            )
        )
        before = len(fixture["client"].comments)
        result = reconcile_required_projection(
            fixture["target"], fixture["snapshot"], fixture["manifest"],
            remote_head=HEAD, created_at="ownership-repair",
            authenticated_source=fixture["source"],
            checkpoint_fence=checkpoint_fence,
            legacy_unresolved_relation_heads=fixture["unresolved"],
        )
        self.assertEqual(len(result["writes"]), 1)
        self.assertEqual(len(fixture["client"].comments), before + 1)
        self.assertEqual(fixture["target"].state().revision, 23)
        self.assertEqual(
            (fixture["target"].state().events[-1]["kind"],
             fixture["target"].state().events[-1]["key"]),
            ("scope", "root"),
        )
        self.assertEqual(
            result["disposition"]["recovered_from_checkpoint"], checkpoint_id,
        )
        after_first_write = len(fixture["client"].comments)
        replay_manifest = {
            **projection_review_contract(fixture["target"].state()),
            "retirements": [],
            "projection": deepcopy(fixture["manifest"]["projection"]),
        }
        replay = reconcile_required_projection(
            fixture["target"], fixture["snapshot"], replay_manifest,
            remote_head=HEAD, created_at="ownership-repair-replay",
            authenticated_source=fixture["source"],
            checkpoint_fence=checkpoint_fence,
            legacy_unresolved_relation_heads=fixture["unresolved"],
        )
        self.assertEqual(replay["writes"], [])
        self.assertEqual(len(fixture["client"].comments), after_first_write)

    def test_generation_checkpoint_wrong_plan_fence_refuses_zero_write(self):
        fixture = self.generation_checkpoint_ownership_repair_fixture()
        before = len(fixture["client"].comments)

        def wrong_plan_fence():
            return workstream_projection.latest_acknowledged_checkpoint_id_from_comments(
                fixture["client"].comments, workstream_id="GEN-37",
                plan_revision="b" * 64, authenticated_route=AUTHORITY,
            )

        with self.assertRaisesRegex(
            LinearProjectionError,
            "checkpoint_authority_changed_reload_required",
        ):
            reconcile_required_projection(
                fixture["target"], fixture["snapshot"], fixture["manifest"],
                remote_head=HEAD, created_at="wrong-plan-fence",
                authenticated_source=fixture["source"],
                checkpoint_fence=wrong_plan_fence,
                legacy_unresolved_relation_heads=fixture["unresolved"],
            )
        self.assertEqual(len(fixture["client"].comments), before)

    def test_generation_checkpoint_duplicate_carried_evidence_refuses_zero_write(self):
        fixture = self.generation_checkpoint_ownership_repair_fixture()
        fixture["client"].comments.append({
            "id": "contradictory-carried-checkpoint",
            "body": encode_checkpoint_comment(fixture["checkpoint"]),
        })
        before = len(fixture["client"].comments)

        def contradictory_fence():
            return workstream_projection.latest_acknowledged_checkpoint_id_from_comments(
                fixture["client"].comments, workstream_id="GEN-37",
                plan_revision=PLAN, authenticated_route=AUTHORITY,
            )

        with self.assertRaisesRegex(
            LinearCheckpointError, "duplicate_checkpoint_event_id",
        ):
            reconcile_required_projection(
                fixture["target"], fixture["snapshot"], fixture["manifest"],
                remote_head=HEAD, created_at="contradictory-carried-evidence",
                authenticated_source=fixture["source"],
                checkpoint_fence=contradictory_fence,
                legacy_unresolved_relation_heads=fixture["unresolved"],
            )
        self.assertEqual(len(fixture["client"].comments), before)

    def test_generation_checkpoint_ownership_repair_refuses_changed_authority_zero_write(self):
        def checkpoint_fence(fixture):
            return workstream_projection.latest_acknowledged_checkpoint_id_from_comments(
                fixture["client"].comments, workstream_id="GEN-37",
                plan_revision=PLAN, authenticated_route=AUTHORITY,
            )

        def successor(fixture, boundary_id):
            checkpoint = fixture["checkpoint"]
            return build_checkpoint(
                workstream_id="GEN-37", boundary_id=boundary_id,
                root_revision=1, plan_revision=PLAN,
                before_status="In Progress", after_status="In Progress",
                execution=deepcopy(checkpoint["execution"]), exact_head=HEAD,
                evidence=[], blocker=None, next_action="Continue newer work.",
                predecessor_event_id=checkpoint["event_id"],
            )

        def alter_selected_transition(fixture):
            for comment in fixture["client"].comments:
                matches = projection_module.PROJECTION_RE.findall(
                    comment.get("body") or ""
                )
                if not matches:
                    continue
                event = projection_module._decode_projection(matches[0])
                if event["kind"] != "generation_transition":
                    continue
                previous = event["value"]["activation_checkpoint"]
                replacement = build_checkpoint(
                    workstream_id=previous["workstream_id"],
                    boundary_id=previous["boundary_id"],
                    root_revision=previous["root_revision"],
                    plan_revision=previous["plan_revision"],
                    before_status=previous["status"]["before"],
                    after_status=previous["status"]["after"],
                    execution=deepcopy(previous["execution"]),
                    exact_head=previous["exact_head"],
                    evidence=deepcopy(previous["evidence"]),
                    blocker=deepcopy(previous["blocker"]),
                    next_action="Altered checkpoint authority.",
                    predecessor_event_id=previous["predecessor_event_id"],
                )
                event["value"]["activation_checkpoint"] = replacement
                ids = event["value"]["to"]["checkpoint_event_ids"]
                event["value"]["to"]["checkpoint_event_ids"] = [
                    replacement["event_id"] if item == previous["event_id"] else item
                    for item in ids
                ]
                event["value"]["to"]["checkpoint_events_sha256"] = hashlib.sha256(
                    json.dumps(
                        event["value"]["to"]["checkpoint_event_ids"],
                        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                event["value"]["activation_checkpoint_sha256"] = hashlib.sha256(
                    json.dumps(
                        replacement, ensure_ascii=False, sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                event["event_id"] = projection_module._event_id(event)
                comment["body"] = encode_projection_comment(event)
                return
            self.fail("generation transition missing from fixture")

        def append_ambiguous_successors(fixture):
            for index in range(2):
                checkpoint = successor(fixture, f"competing-{index}")
                fixture["client"].comments.append({
                    "id": f"competing-checkpoint-{index}",
                    "body": encode_checkpoint_comment(checkpoint),
                })

        def append_stale_successor(fixture):
            checkpoint = successor(fixture, "newer-authority")
            fixture["client"].comments.append({
                "id": "newer-checkpoint",
                "body": encode_checkpoint_comment(checkpoint),
            })

        cases = (
            ("altered", alter_selected_transition,
             "checkpoint_authority_changed_reload_required"),
            ("ambiguous", append_ambiguous_successors,
             "checkpoint_revision_not_monotonic"),
            ("stale", append_stale_successor,
             "checkpoint_authority_changed_reload_required"),
        )
        for name, mutate, expected_error in cases:
            with self.subTest(authority=name):
                fixture = self.generation_checkpoint_ownership_repair_fixture()
                mutate(fixture)
                before = len(fixture["client"].comments)
                with self.assertRaisesRegex(
                    (LinearProjectionError, LinearCheckpointError),
                    expected_error,
                ):
                    reconcile_required_projection(
                        fixture["target"], fixture["snapshot"],
                        fixture["manifest"], remote_head=HEAD,
                        created_at=f"refuse-{name}",
                        authenticated_source=fixture["source"],
                        checkpoint_fence=lambda: checkpoint_fence(fixture),
                        legacy_unresolved_relation_heads=fixture["unresolved"],
                    )
                self.assertEqual(len(fixture["client"].comments), before)

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
            "latest_checkpoint": acknowledged_checkpoint("wsc-live"),
        }
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        first = reconcile_required_projection(
            adapter, snapshot, manifest, remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
            checkpoint_fence=lambda: "wsc-live",
        )
        self.assertEqual(first["disposition"]["disposition"], "attach")
        self.assertTrue(first["readback_verified"])
        self.assertEqual(len(first["writes"]), 4)
        manifest = reviewed_manifest(adapter, projection)
        second = reconcile_required_projection(
            adapter, snapshot, manifest, remote_head=HEAD,
            created_at="2026-08-27T18:01:00Z", authenticated_source=source,
            checkpoint_fence=lambda: "wsc-live",
        )
        self.assertEqual(second["writes"], [])
        self.assertEqual(len(client.comments), 4)

    def test_product_reconcile_cas_repairs_stale_checkpoint_disposition_and_replays_noop(self):
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
                "agent": "codex", "machine": "M5",
                "session_id": "session-m5",
                "worktree": {"state": "safe", "head": HEAD},
            }},
        ]
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        old_checkpoint = "wsc_" + "e" * 32
        new_checkpoint = "wsc_" + "3" * 32
        reconcile_required_projection(
            adapter,
            {"root": {"identifier": "GEN-37"},
             "latest_checkpoint": acknowledged_checkpoint(old_checkpoint)},
            reviewed_manifest(adapter, projection), remote_head=HEAD,
            created_at="2026-08-29T20:00:00Z",
            authenticated_source=source,
            checkpoint_fence=lambda: old_checkpoint,
        )
        old_disposition_event = next(
            event for event in reversed(adapter.state().events)
            if (event["kind"], event["key"]) == ("disposition", "root")
        )
        refreshed_snapshot = {
            "root": {"identifier": "GEN-37"},
            "latest_checkpoint": acknowledged_checkpoint(new_checkpoint),
        }
        comments_before = len(client.comments)
        repaired = reconcile_required_projection(
            adapter, refreshed_snapshot,
            reviewed_manifest(adapter, projection), remote_head=HEAD,
            created_at="2026-08-29T20:01:00Z",
            authenticated_source=source,
            checkpoint_fence=lambda: new_checkpoint,
        )
        self.assertEqual(len(repaired["writes"]), 1)
        self.assertEqual(len(client.comments), comments_before + 1)
        repaired_event = adapter.state().events[-1]
        self.assertEqual(
            (repaired_event["kind"], repaired_event["key"]),
            ("disposition", "root"),
        )
        self.assertEqual(
            repaired_event["supersedes_event_id"],
            old_disposition_event["event_id"],
        )
        self.assertEqual(
            repaired["disposition"]["recovered_from_checkpoint"],
            new_checkpoint,
        )
        replay = reconcile_required_projection(
            adapter, refreshed_snapshot,
            reviewed_manifest(adapter, projection), remote_head=HEAD,
            created_at="2026-08-29T20:02:00Z",
            authenticated_source=source,
            checkpoint_fence=lambda: new_checkpoint,
        )
        self.assertEqual(replay["writes"], [])
        self.assertEqual(len(client.comments), comments_before + 1)

    def test_product_reconcile_refuses_checkpoint_advance_before_cas_write(self):
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
                "agent": "codex", "machine": "M5",
                "session_id": "session-m5",
                "worktree": {"state": "safe", "head": HEAD},
            }},
        ]
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        old_checkpoint = "wsc_" + "e" * 32
        new_checkpoint = "wsc_" + "3" * 32
        newer_checkpoint = "wsc_" + "4" * 32
        reconcile_required_projection(
            adapter,
            {"root": {"identifier": "GEN-37"},
             "latest_checkpoint": acknowledged_checkpoint(old_checkpoint)},
            reviewed_manifest(adapter, projection), remote_head=HEAD,
            created_at="2026-08-29T20:00:00Z",
            authenticated_source=source,
            checkpoint_fence=lambda: old_checkpoint,
        )
        observations = iter((new_checkpoint, newer_checkpoint))
        comments_before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "checkpoint_authority_changed_reload_required",
        ):
            reconcile_required_projection(
                adapter,
                {"root": {"identifier": "GEN-37"},
                 "latest_checkpoint": acknowledged_checkpoint(new_checkpoint)},
                reviewed_manifest(adapter, projection), remote_head=HEAD,
                created_at="2026-08-29T20:01:00Z",
                authenticated_source=source,
                checkpoint_fence=lambda: next(observations),
            )
        self.assertEqual(len(client.comments), comments_before)

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
            "latest_checkpoint": acknowledged_checkpoint("wsc-live"),
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
            checkpoint_fence=lambda: "wsc-live",
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
            checkpoint_fence=lambda: "wsc-live",
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
            {"identity": exact, "sha256": "new", "bytes": 10},
        )

        self.assertEqual(synced["projection"][0]["value"], {
            "identity": exact, "sha256": "new",
        })
        self.assertEqual(source, {"identity": exact, "sha256": "new", "bytes": 10})
        self.assertEqual(manifest["projection"][0]["value"]["sha256"], "old")

    def test_manifest_source_sync_updates_equivalent_ref_to_exact_canonical(self):
        main = "https://github.com/acme/plans/blob/main/PLAN.md"
        exact = "https://github.com/acme/plans/blob/" + "a" * 40 + "/PLAN.md"
        manifest = {"projection": [{
            "kind": "source", "key": "root",
            "value": {"identity": main, "sha256": PLAN},
        }]}

        synced, source = workstream_projection.synchronize_manifest_source(
            manifest, f"Canonical plan: {exact}",
            {"identity": exact, "sha256": PLAN, "bytes": 10},
        )

        self.assertEqual(synced["projection"][0]["value"], {
            "identity": exact, "sha256": PLAN,
        })
        self.assertEqual(source, {
            "identity": exact, "sha256": PLAN, "bytes": 10,
        })

    def test_terminal_source_transition_is_partial_replayable_and_narrow(self):
        client, adapter, _source, graph, children, _manifest = (
            self.multi_terminal_repair_fixture(evidence_active=False)
        )
        main = "https://github.com/acme/plans/blob/" + "b" * 40 + "/PLAN.md"
        exact = "https://github.com/acme/plans/blob/" + "a" * 40 + "/PLAN.md"
        current_source = next(
            event for event in reversed(adapter.state().events)
            if event["kind"] == "source"
        )
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"identity": main, "sha256": PLAN}, plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-29T01:00:00Z",
            supersedes_event_id=current_source["event_id"], authority=AUTHORITY,
        ))
        active = adapter.state()
        active_heads = workstream_projection._active_heads(active)
        desired = [
            {"kind": event["kind"], "key": event["key"],
             "value": deepcopy(event["value"])}
            for (kind, _key), event in active_heads.items()
            if kind in {"scope", "source", "provenance"}
        ]
        pending = [{
            "child_identifier": child["identifier"],
            "child_issue_id": child["id"],
            "expected_child_readback_sha256": canonical_digest(
                terminal_child_readback(child)
            ),
            "expected_assignee_id": (child.get("assignee") or {}).get("id"),
        } for child in children]
        transition = {
            "from_identity": main, "to_identity": exact, "sha256": PLAN,
            "created_at": "2026-08-29T01:01:00Z",
            "expected_revision": active.revision,
            "from_event_id": active_heads[("source", "root")]["event_id"],
            "from_value_sha256": canonical_digest(
                active_heads[("source", "root")]["value"]
            ),
            "pending_children": pending,
        }
        manifest = {
            **reviewed_manifest(adapter, desired),
            "terminal_child_source_transition": transition,
        }
        graph = deepcopy(graph)
        graph["root"]["description"] = f"Canonical plan: {exact}"
        manifest, authenticated = workstream_projection.synchronize_manifest_source(
            manifest, graph["root"]["description"],
            {"identity": exact, "sha256": PLAN},
        )
        prepared = workstream_projection.prepare_terminal_child_source_transition(
            manifest, graph, adapter.state(),
        )
        mixed_events = deepcopy(list(adapter.state().events))
        mixed_source = next(
            event for event in reversed(mixed_events)
            if event["event_id"] == transition["from_event_id"]
        )
        mixed_source["schema_version"] = 1
        mixed_source.pop("authority")
        mixed_state = SimpleNamespace(
            revision=len(mixed_events), events=tuple(mixed_events),
            snapshot=deepcopy(adapter.state().snapshot),
        )
        mixed_manifest = deepcopy(manifest)
        mixed_manifest.update(projection_review_contract(mixed_state))
        writes_before_mixed_refusal = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_source_transition_requires_v2_source_predecessor",
        ):
            workstream_projection.prepare_terminal_child_source_transition(
                mixed_manifest, graph, mixed_state,
            )
        self.assertEqual(len(client.comments), writes_before_mixed_refusal)
        drifted_graph = deepcopy(graph)
        drifted_graph["children"][1]["status_type"] = "started"
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_source_transition_pending_set_mismatch",
        ):
            workstream_projection.prepare_terminal_child_source_transition(
                manifest, drifted_graph, adapter.state(),
            )
        bad_url = deepcopy(manifest)
        bad_url["terminal_child_source_transition"]["to_identity"] = (
            "https://github.com/acme/plans/blob/develop/PLAN.md"
        )
        with self.assertRaisesRegex(
            LinearProjectionError, "terminal_child_source_transition_invalid_route",
        ):
            workstream_projection.prepare_terminal_child_source_transition(
                bad_url, graph, adapter.state(),
            )
        bad_digest = deepcopy(manifest)
        bad_digest["terminal_child_source_transition"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(
            LinearProjectionError, "terminal_child_source_transition_invalid_route",
        ):
            workstream_projection.prepare_terminal_child_source_transition(
                bad_digest, graph, adapter.state(),
            )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=authenticated,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = {item["child_identifier"]: item["expected_child_readback_sha256"]
                    for item in pending}
        legacy_state = deepcopy(adapter.state())
        for event in legacy_state.events:
            event["schema_version"] = 1
        legacy_contract = projection_review_contract(legacy_state)
        legacy_manifest = deepcopy(prepared)
        for key, value in legacy_contract.items():
            legacy_manifest[key] = deepcopy(value)
        writes_before_legacy_refusal = len(client.comments)
        with mock.patch.object(adapter, "state", return_value=legacy_state):
            with self.assertRaisesRegex(
                LinearProjectionError,
                "terminal_child_source_transition_requires_v2_projection",
            ):
                reconcile_required_projection(
                    adapter, preview, legacy_manifest, remote_head=HEAD,
                    created_at="2026-08-29T01:00:30Z",
                    authenticated_source=authenticated,
                    relation_target_resolver=self.relation_target_resolver,
                    terminal_child_fence=lambda ids: {
                        item: expected[item] for item in ids
                    },
                    legacy_unresolved_relation_heads=unresolved,
                )
        self.assertEqual(len(client.comments), writes_before_legacy_refusal)
        before = adapter.state().revision
        original_execute = client.execute

        def crash_after_source_append(query, variables):
            response = original_execute(query, variables)
            if "commentCreate" in query:
                encoded = variables.get("input", {}).get("body", "")
                matches = projection_module.PROJECTION_RE.findall(encoded)
                if matches and (
                    projection_module._decode_projection(matches[0])["kind"]
                    == "source"
                ):
                    raise SystemExit("simulated source transition caller death")
            return response

        client.execute = crash_after_source_append
        try:
            with self.assertRaisesRegex(
                SystemExit, "simulated source transition caller death",
            ):
                reconcile_required_projection(
                    adapter, preview, prepared, remote_head=HEAD,
                    created_at="2026-08-29T01:01:00Z",
                    authenticated_source=authenticated,
                    relation_target_resolver=self.relation_target_resolver,
                    terminal_child_fence=lambda ids: {
                        item: expected[item] for item in ids
                    },
                    legacy_unresolved_relation_heads=unresolved,
                )
        finally:
            client.execute = original_execute
        self.assertEqual(
            [(event["kind"], event["key"])
             for event in adapter.state().events[before:]],
            [("source", "root")],
        )
        reviewed_predecessor = next(
            event for event in reversed(adapter.state().events[:before])
            if event["kind"] == "source" and event["key"] == "root"
        )
        forged_event = build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"identity": exact, "sha256": PLAN},
            plan_revision=PLAN, expected_revision=before,
            created_at="same-value-wrong-envelope",
            supersedes_event_id=reviewed_predecessor["event_id"],
            authority=AUTHORITY,
        )
        forged_state = SimpleNamespace(
            revision=before + 1,
            events=tuple([
                *adapter.state().events[:before], forged_event,
            ]),
            snapshot=deepcopy(adapter.state().snapshot),
        )
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_source_transition_replay_event_mismatch",
        ):
            workstream_projection.prepare_terminal_child_source_transition(
                manifest, graph, forged_state,
            )
        replay = workstream_projection.prepare_terminal_child_source_transition(
            manifest, graph, adapter.state(),
        )
        self.assertEqual(
            replay["expected_projection_revision"], adapter.state().revision,
        )
        replay_preview, replay_unresolved = (
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", replay, adapter,
                authenticated_route=AUTHORITY,
                authenticated_source=authenticated, remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        )
        result = reconcile_required_projection(
            adapter, replay_preview, replay, remote_head=HEAD,
            created_at="2026-08-29T01:01:00Z",
            authenticated_source=authenticated,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda ids: {
                item: expected[item] for item in ids
            },
            legacy_unresolved_relation_heads=replay_unresolved,
        )
        self.assertEqual(result["writes"], [])
        self.assertFalse(result["resume_authority_verified"])

        comments = mock.Mock()
        comments.comments.side_effect = lambda: [
            dict(item) for item in client.comments
        ]
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = (
            live_graph_with_empty_child_comments(graph)
        )
        with tempfile.TemporaryDirectory() as directory:
            manifest_path = Path(directory) / "source-transition.json"
            manifest_path.write_text(json.dumps(manifest))
            output = io.StringIO()
            argv = [
                "workstream_projection.py", "GEN-37", str(manifest_path),
                "--remote-head", HEAD, "--plan-source", "ignored",
                "--plan-identity", exact,
                "--max-bytes", "65536", "--max-items", "500",
                "--apply", "--created-at", "2026-08-29T01:01:00Z",
                "--expected-preview-sha256", "a" * 64,
            ]
            inactive_binding = {
                "mode": "inactive_candidate", "selected": None,
                "requested_plan_revision": PLAN,
                "controlled_plan_revisions": [PLAN],
            }
            with mock.patch.object(workstream_projection.sys, "argv", argv), \
                 mock.patch.object(workstream_projection.sys, "stdout", output), \
                 mock.patch.object(workstream_projection, "plan_payload", return_value={
                     "source": {"identity": exact, "sha256": PLAN}
                 }), \
                 mock.patch.object(workstream_projection, "load_linear_api_key", return_value="secret"), \
                 mock.patch.object(workstream_projection, "HttpGraphQLClient", return_value=client), \
                 mock.patch.object(workstream_projection, "resolve_linear_route", return_value=(AUTHORITY, None)), \
                 mock.patch.object(workstream_projection, "resolve_authenticated_issue_route", return_value=AUTHORITY), \
                 mock.patch.object(workstream_projection, "projection_generation_source_binding", return_value=inactive_binding), \
                 mock.patch.object(workstream_projection, "LinearGraphQLTransport", return_value=transport), \
                 mock.patch.object(workstream_projection, "LinearCommentEventAdapter", return_value=comments):
                self.assertEqual(workstream_projection.main(), 0)
            payload = json.loads(output.getvalue())
            self.assertEqual(payload["operation_status"], "partial")
            self.assertEqual(
                payload["resume_authority"],
                "partial_terminal_closure_required",
            )
            self.assertFalse(payload["resume_authority_verified"])
            self.assertEqual(
                payload["pending_terminal_closure"], ["GEN-70", "GEN-72"],
            )
            self.assertTrue(payload["source_transition"]["verified"])
            self.assertEqual(payload["writes"], [])

            changed_graph = deepcopy(graph)
            changed_graph["children"][1]["assignee"] = {
                "id": "81818181-8181-4181-8181-818181818181"
            }
            transport.snapshot_for_root.side_effect = [
                *[live_graph_with_empty_child_comments(graph) for _ in range(3)],
                *[
                    live_graph_with_empty_child_comments(changed_graph)
                    for _ in range(3)
                ],
            ]
            error = io.StringIO()
            with mock.patch.object(workstream_projection.sys, "argv", argv), \
                 mock.patch.object(workstream_projection.sys, "stderr", error), \
                 mock.patch.object(workstream_projection, "plan_payload", return_value={
                     "source": {"identity": exact, "sha256": PLAN}
                 }), \
                 mock.patch.object(workstream_projection, "load_linear_api_key", return_value="secret"), \
                 mock.patch.object(workstream_projection, "HttpGraphQLClient", return_value=client), \
                 mock.patch.object(workstream_projection, "resolve_linear_route", return_value=(AUTHORITY, None)), \
                 mock.patch.object(workstream_projection, "resolve_authenticated_issue_route", return_value=AUTHORITY), \
                 mock.patch.object(workstream_projection, "projection_generation_source_binding", return_value=inactive_binding), \
                 mock.patch.object(workstream_projection, "LinearGraphQLTransport", return_value=transport), \
                 mock.patch.object(workstream_projection, "LinearCommentEventAdapter", return_value=comments):
                self.assertEqual(workstream_projection.main(), 2)
            self.assertIn(
                "terminal_child_readback_changed_reload_required:GEN-70",
                error.getvalue(),
            )

        widened = deepcopy(manifest)
        next(item for item in widened["projection"] if item["kind"] == "scope")[
            "value"
        ]["namespace"] = "forged"
        with self.assertRaisesRegex(
            LinearProjectionError, "terminal_child_source_transition_unrelated_change",
        ):
            workstream_projection.prepare_terminal_child_source_transition(
                widened, graph, adapter.state(),
            )

    def test_manifest_source_sync_refuses_different_authenticated_exact_ref(self):
        first = "https://github.com/acme/plans/blob/" + "a" * 40 + "/PLAN.md"
        second = "https://github.com/acme/plans/blob/" + "b" * 40 + "/PLAN.md"
        manifest = {"projection": [{
            "kind": "source", "key": "root",
            "value": {"identity": first, "sha256": PLAN},
        }]}

        with self.assertRaisesRegex(
            LinearProjectionError, "plan_source_conflicts_canonical_issue_url",
        ):
            workstream_projection.synchronize_manifest_source(
                manifest, f"Canonical plan: {second}",
                {"identity": first, "sha256": PLAN, "bytes": 10},
            )

    def test_canonical_source_readback_refuses_equivalent_ref_change(self):
        first = "https://github.com/acme/plans/blob/" + "a" * 40 + "/PLAN.md"
        second = "https://github.com/acme/plans/blob/" + "b" * 40 + "/PLAN.md"

        with self.assertRaisesRegex(
            LinearProjectionError, "canonical_plan_changed_during_projection",
        ):
            workstream_projection.validate_canonical_source_readback(
                f"Canonical plan: {second}",
                {"identity": first, "sha256": PLAN},
            )

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

    def test_generation_candidate_source_independence_composed_and_fail_closed(self):
        raw = b"# Exact generation candidate\n"
        target_plan = hashlib.sha256(raw).hexdigest()
        predecessor_plan = "b" * 64
        old_identity = (
            "https://github.com/acme/plans/blob/" + "c" * 40 + "/PLAN.md"
        )
        target_identity = (
            "https://github.com/acme/plans/blob/" + "d" * 40 + "/PLAN.md"
        )
        route = {"workspace_id": "workspace", "team_id": "team",
                 "project_id": "project", "root_issue_id": ROOT_UUID}

        def append_generation(client, plan_revision, identity):
            adapter = LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=plan_revision, **AUTHORITY,
            )
            values = [
                ("scope", "root", scope()),
                ("source", "root", {
                    "identity": identity, "sha256": plan_revision,
                }),
                ("provenance", "generation", {
                    "agent": "codex", "machine": "M5", "session_id": plan_revision[:8],
                    "worktree": {"state": "safe", "head": HEAD},
                }),
                ("disposition", "root", {
                    "disposition": "attach", "remote_head": HEAD,
                    "recovered_from_checkpoint": None,
                }),
            ]
            for kind, key, value in values:
                adapter.append(build_projection_event(
                    workstream_id="GEN-37", kind=kind, key=key, value=value,
                    plan_revision=plan_revision,
                    expected_revision=adapter.state().revision,
                    created_at=f"candidate-{adapter.state().revision}",
                    authority=AUTHORITY,
                ))
            return adapter

        def graph(description):
            return {
                "root": {
                    "identifier": "GEN-37", "url": "https://linear/GEN-37",
                    "description": description, "plan_revision": predecessor_plan,
                    "revision": 0, "status": "In Progress",
                    "next_action": "continue",
                },
                "children": [{
                    "identifier": "GEN-38", "title": "Candidate child",
                    "status": "In Progress", "next_action": "continue",
                }],
                "decisions": [],
            }

        def manifest_for(client, identity=target_identity):
            target = LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=target_plan, **AUTHORITY,
            )
            return {
                **projection_review_contract(target.state()),
                "retirements": [],
                "projection": [
                    {"kind": "scope", "key": "root", "value": scope()},
                    {"kind": "source", "key": "root", "value": {
                        "identity": identity, "sha256": target_plan,
                    }},
                    {"kind": "provenance", "key": "candidate", "value": {
                        "agent": "codex", "machine": "M5",
                        "session_id": "candidate",
                        "worktree": {"state": "safe", "head": HEAD},
                    }},
                ],
            }

        def invoke(client, manifest, live_graph, identity, *, graph_side_effect=None):
            comments = mock.Mock()
            comments.comments.side_effect = lambda: [
                dict(item) for item in client.comments
            ]
            transport = mock.Mock()
            if graph_side_effect is None:
                transport.snapshot_for_root.return_value = (
                    live_graph_with_empty_child_comments(live_graph)
                )
            else:
                transport.snapshot_for_root.side_effect = [
                    live_graph_with_empty_child_comments(item)
                    for item in graph_side_effect
                ]
            with tempfile.TemporaryDirectory() as directory:
                plan_path = Path(directory) / "plan.md"
                manifest_path = Path(directory) / "manifest.json"
                plan_path.write_bytes(raw)
                manifest_path.write_text(json.dumps(manifest))
                argv = [
                    "workstream_projection.py", "GEN-37", str(manifest_path),
                    "--remote-head", HEAD, "--plan-source", str(plan_path),
                    "--plan-identity", identity,
                    "--apply", "--created-at", "2026-08-31T23:00:00Z",
                    "--expected-preview-sha256", "a" * 64,
                ]
                stdout, stderr = io.StringIO(), io.StringIO()
                with mock.patch.object(workstream_projection.sys, "argv", argv), \
                     mock.patch.object(workstream_projection.sys, "stdout", stdout), \
                     mock.patch.object(workstream_projection.sys, "stderr", stderr), \
                     mock.patch.object(workstream_projection, "load_linear_api_key", return_value="secret"), \
                     mock.patch.object(workstream_projection, "HttpGraphQLClient", return_value=client), \
                     mock.patch.object(workstream_projection, "resolve_linear_route", return_value=(route, None)), \
                     mock.patch.object(workstream_projection, "resolve_authenticated_issue_route", return_value=route), \
                     mock.patch.object(workstream_projection, "LinearGraphQLTransport", return_value=transport), \
                     mock.patch.object(workstream_projection, "LinearCommentEventAdapter", return_value=comments):
                    code = workstream_projection.main()
            return code, stdout.getvalue(), stderr.getvalue()

        client = FakeProjectionClient()
        predecessor = append_generation(client, predecessor_plan, old_identity)
        description = f"Canonical plan: {old_identity}\nKeep this diagnostic."
        before = len(client.comments)
        code, output, error = invoke(
            client, manifest_for(client), graph(description), target_identity,
        )
        self.assertEqual((code, error), (0, ""))
        receipt = json.loads(output)
        self.assertEqual(receipt["source_sync"]["identity"], target_identity)
        self.assertEqual(
            receipt["canonical_description_fence"],
            workstream_projection.canonical_source_diagnostic_fence(description),
        )
        self.assertGreater(len(client.comments), before)
        self.assertFalse(any("issueUpdate" in query for query, _ in client.calls))

        for label, bad_description, bad_identity in (
            (
                "document", description,
                "https://github.com/acme/other/blob/" + "d" * 40 + "/OTHER.md",
            ),
            (
                "ambiguous",
                f"Canonical plan: {old_identity}\nCanonical plan: https://example.test/other",
                target_identity,
            ),
        ):
            failed = FakeProjectionClient()
            append_generation(failed, predecessor_plan, old_identity)
            count = len(failed.comments)
            bad_manifest = manifest_for(failed, bad_identity)
            code, _output, error = invoke(
                failed, bad_manifest, graph(bad_description), bad_identity,
            )
            with self.subTest(label=label):
                self.assertEqual(code, 2)
                self.assertEqual(len(failed.comments), count)
                self.assertFalse(any("issueUpdate" in query for query, _ in failed.calls))

        for label, mutate in (
            (
                "missing_source",
                lambda value: value["projection"].__setitem__(
                    slice(1, 2), [],
                ),
            ),
            (
                "source_sha",
                lambda value: value["projection"][1]["value"].__setitem__(
                    "sha256", "0" * 64,
                ),
            ),
            (
                "review_contract",
                lambda value: value.__setitem__(
                    "expected_projection_revision", 1,
                ),
            ),
        ):
            failed = FakeProjectionClient()
            append_generation(failed, predecessor_plan, old_identity)
            bad_manifest = manifest_for(failed)
            mutate(bad_manifest)
            count = len(failed.comments)
            code, _output, _error = invoke(
                failed, bad_manifest, graph(description), target_identity,
            )
            with self.subTest(label=label):
                self.assertEqual(code, 2)
                self.assertEqual(len(failed.comments), count)
                self.assertFalse(any("issueUpdate" in query for query, _ in failed.calls))

        changed = graph(description + "\nConcurrent edit.")
        failed = FakeProjectionClient()
        append_generation(failed, predecessor_plan, old_identity)
        count = len(failed.comments)
        code, _output, error = invoke(
            failed, manifest_for(failed), graph(description), target_identity,
            graph_side_effect=[
                graph(description), graph(description), graph(description),
                changed,
            ],
        )
        self.assertEqual(code, 2)
        self.assertIn("canonical_plan_changed_during_projection", error)
        self.assertEqual(len(failed.comments), count)

        def candidate_loader(plan_revision):
            state = LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=plan_revision, **AUTHORITY,
            ).state()
            source = state.snapshot["source"]
            material = reduce_event_comments(client.comments, workstream_id="GEN-37")
            checkpoints = reduce_checkpoint_comments(
                client.comments, workstream_id="GEN-37",
            )
            checkpoint_ids = sorted(
                item["event_id"] for item in checkpoints.checkpoints
                if item["plan_revision"] == plan_revision
            )
            return {
                "resume_authority": "full", "plan_revision": plan_revision,
                "authenticated_route": AUTHORITY, "source": source,
                "material_revision": material.revision,
                "checkpoint_event_ids": checkpoint_ids,
                "projection_revision": state.revision,
                "graph_frontier_sha256": generation_digest("graph"),
                "snapshot_sha256": generation_digest([
                    event["event_id"] for event in state.events
                ]),
                "quarantined_legacy_writes": generation_quarantine_metadata(
                    client.comments, workstream_id="GEN-37",
                ),
            }

        old_state = predecessor.state()
        generation_transport = GenerationTransport(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            authority=AUTHORITY, candidate_loader=candidate_loader,
            legacy_description_plan_revision=predecessor_plan,
        )
        generation_transport._capability_checked = True
        generation_transport.activate(
            target_plan_revision=target_plan, created_at="activate",
            retirement=build_retirement_proof(
                predecessor_plan_revision=predecessor_plan,
                retired_at="activate", retired_writer_epoch=0,
                provenance_event_ids=[
                    event["event_id"] for event in old_state.events
                    if event["kind"] == "provenance"
                ], checkpoint_event_ids=[],
            ),
        )
        active_count = len(client.comments)
        attacker_identity = (
            "https://github.com/acme/plans/blob/" + "e" * 40 + "/PLAN.md"
        )
        active_manifest = manifest_for(client, attacker_identity)
        code, _output, error = invoke(
            client, active_manifest, graph(description), attacker_identity,
        )
        self.assertEqual(code, 2)
        self.assertIn("active_projection_source_mismatch", error)
        self.assertEqual(len(client.comments), active_count)
        self.assertFalse(any("issueUpdate" in query for query, _ in client.calls))

    def test_predecessor_seed_accepts_only_authenticated_15_v1_67_v2_history(self):
        client = FakeProjectionClient()
        client.comments = deepcopy(gen37_040_activation_fixture())
        activation = projection_module._decode_projection(
            projection_module.PROJECTION_RE.findall(client.comments[-1]["body"])[0]
        )
        route = activation["authority"]
        for revision in range(16, 82):
            event = build_projection_event(
                workstream_id="GEN-37", kind="provenance",
                key=f"v2-{revision}", value={
                    "agent": "codex", "machine": "M5",
                    "session_id": f"v2-{revision}",
                }, plan_revision=PLAN, expected_revision=revision,
                created_at=f"2026-08-29T22:{revision:02d}:00Z",
                authority=route,
            )
            client.comments.append({
                "id": projection_slot_id("GEN-37", PLAN, revision, route),
                "body": encode_projection_comment(event),
            })
        material = Delta(
            "mixed-history-material", "GEN-37", "requirement", "reviewer",
            {"text": "Bind the authenticated mixed predecessor."}, 0,
            "2026-08-29T23:00:00Z",
        )
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="mixed-history",
            root_revision=1, plan_revision=PLAN,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "mixed-history", "machine": "M5",
                "worktree": {
                    "state": "safe", "path": "/worktree",
                    "branch": "mixed", "head": HEAD,
                },
            }, exact_head=HEAD, evidence=[], blocker=None,
            next_action="Project the candidate generation.",
        )
        client.comments.extend([
            {"id": "mixed-material", "body": encode_event_comment(material)},
            {"id": "mixed-checkpoint", "body": encode_checkpoint_comment(checkpoint)},
        ])
        accepted = list(reduce_projection_comments(
            client.comments, workstream_id="GEN-37",
            expected_plan_revision=PLAN, authenticated_route=route,
        ).events)
        self.assertEqual(sum(event["schema_version"] == 1 for event in accepted), 15)
        self.assertEqual(sum(event["schema_version"] == 2 for event in accepted), 67)
        current_plan = "c" * 64
        desired_scope = scope()
        desired_scope["linear"] = {
            **desired_scope["linear"], **route,
            "route_verification": {
                **route, "observed_at": "2026-08-29T22:00:00Z",
                "evidence": [{
                    "kind": "authenticated_linear_readback",
                    "authenticated": True, **route,
                }],
            },
        }
        snapshot = {
            "root": {"identifier": "GEN-37", "plan_revision": current_plan},
            "children": [],
        }

        def contract(comments):
            state = reduce_projection_comments(
                comments, workstream_id="GEN-37",
                expected_plan_revision=current_plan, authenticated_route=route,
            )
            return terminal_child_evidence_seed_predecessor_contract(
                snapshot, state, comments, workstream_id="GEN-37",
                predecessor_plan_revision=PLAN, desired_scope=desired_scope,
                seeds=[], desired_contracts={},
            )[0]

        binding = contract(client.comments)
        self.assertEqual(binding["projection_revision"], 82)
        self.assertEqual(binding["projection_events_sha256"], canonical_digest(accepted))
        self.assertEqual(binding["projection_frontier_event_id"], accepted[-1]["event_id"])

        def replace_activation(comments, mutate):
            changed = deepcopy(comments)
            for index, comment in enumerate(changed):
                match = projection_module.PROJECTION_RE.findall(comment["body"])
                if not match:
                    continue
                event = projection_module._decode_projection(match[0])
                if event["kind"] != "cas_activation":
                    continue
                mutate(event)
                event["event_id"] = projection_module._event_id(event)
                changed[index] = {
                    **comment,
                    "id": projection_slot_id(
                        "GEN-37", PLAN, event["expected_revision"], route,
                    ),
                    "body": encode_projection_comment(event),
                }
                return changed
            raise AssertionError("activation missing")

        def is_activation(comment):
            matches = projection_module.PROJECTION_RE.findall(comment["body"])
            return bool(matches) and projection_module._decode_projection(
                matches[0]
            )["kind"] == "cas_activation"

        invalid_histories = {
            "missing": client.comments[1:],
            "duplicate": [
                *client.comments, {**client.comments[0], "id": "duplicate-legacy"},
            ],
            "missing_activation": [
                comment for comment in client.comments
                if not is_activation(comment)
            ],
            "ordered_ids": replace_activation(
                client.comments,
                lambda event: event["value"]["legacy_event_ids"].reverse(),
            ),
            "digest": replace_activation(
                client.comments,
                lambda event: event["value"].__setitem__(
                    "legacy_events_sha256", "0" * 64,
                ),
            ),
        }
        wrong_route = deepcopy(client.comments)
        last_index = next(
            index for index in range(len(wrong_route) - 1, -1, -1)
            if projection_module.PROJECTION_RE.findall(wrong_route[index]["body"])
        )
        last = projection_module._decode_projection(
            projection_module.PROJECTION_RE.findall(
                wrong_route[last_index]["body"]
            )[0]
        )
        last["authority"] = {**route, "team_id": "wrong-team"}
        last["event_id"] = projection_module._event_id(last)
        wrong_route[last_index] = {
            **wrong_route[last_index],
            "id": projection_slot_id("GEN-37", PLAN, 81, last["authority"]),
            "body": encode_projection_comment(last),
        }
        invalid_histories["wrong_route"] = wrong_route
        for label, comments in invalid_histories.items():
            with self.subTest(label=label), self.assertRaises(LinearProjectionError):
                contract(comments)

        late = build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="late-v2",
            value={"agent": "codex", "machine": "M5", "session_id": "late"},
            plan_revision=PLAN, expected_revision=82,
            created_at="2026-08-29T23:59:00Z", authority=route,
        )
        client.comments.append({
            "id": projection_slot_id("GEN-37", PLAN, 82, route),
            "body": encode_projection_comment(late),
        })
        changed_current = reduce_projection_comments(
            client.comments, workstream_id="GEN-37",
            expected_plan_revision=current_plan, authenticated_route=route,
        )
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_seed_predecessor_history_changed_reload_required",
        ):
            _fence_predecessor_projection_history(changed_current, binding)

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
        transport.snapshot_for_root.return_value = (
            live_graph_with_empty_child_comments(graph)
        )
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
                "--apply", "--created-at", "2026-08-31T23:00:00Z",
                "--expected-preview-sha256", "a" * 64,
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
                frontier = payload["projection_input_frontier"]
                self.assertTrue(frontier["prewrite_verified"])
                self.assertTrue(frontier["postwrite_verified"])
                self.assertFalse(frontier["atomic_with_projection_append"])
                self.assertTrue(frontier["postwrite_verification_required"])
                self.assertTrue(all(
                    receipt["reviewed_projection_input_frontier_sha256"]
                    == frontier["sha256"]
                    for receipt in payload["writes"]
                ))
                self.assertEqual(len(client.comments), expected_writes)
                manifest.update(payload["projection_contract"])
        self.assertEqual(client.comments[0], historical_comment)
        self.assertFalse(any(
            "issueCreate" in query or "issueUpdate" in query
            for query, _variables in client.calls
        ))
        self.assertEqual(len(json.loads(output.getvalue())["writes"]), 0)

    def test_projection_cli_provenance_only_budget_and_child_growth_are_zero_write(self):
        raw = b"# Exact plan\n\n## Deliver\n"
        digest = hashlib.sha256(raw).hexdigest()
        identity = "https://example.test/commit/plan.md"
        source = {"identity": identity, "sha256": digest}
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=digest, **AUTHORITY,
        )
        scoped = scope()
        scoped["child_ownership"] = {}
        scoped["child_ownership"]["GEN-43"] = (
            "github.com:id:R_agent_workstream"
        )
        initial = [
            {"kind": "scope", "key": "root", "value": scoped},
            {"kind": "source", "key": "root", "value": source},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "old",
                "worktree": {"state": "safe", "head": HEAD},
            }},
        ]
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, initial), remote_head=HEAD,
            created_at="2026-08-29T22:00:00Z", authenticated_source=source,
        )
        desired = [
            {"kind": event["kind"], "key": event["key"],
             "value": deepcopy(event["value"])}
            for event in workstream_projection._active_heads(
                adapter.state()
            ).values()
            if event["kind"] != "disposition"
        ]
        provenance = next(
            item for item in desired if item["kind"] == "provenance"
        )
        provenance["value"] = {
            "agent": "codex", "machine": "M5", "session_id": "new",
            "worktree": {"state": "safe", "head": HEAD},
        }
        manifest = reviewed_manifest(adapter, desired)
        graph = live_graph_with_empty_child_comments({
            "root": {
                "identifier": "GEN-37", "url": "https://linear/GEN-37",
                "description": f"Canonical plan: {identity}",
                "plan_revision": digest, "revision": 0,
                "status": "In Progress", "next_action": "continue",
            },
            "children": [{
                "identifier": "GEN-43", "title": "Continuation",
                "url": "https://linear/GEN-43",
                "status": "In Progress", "status_type": "started",
                "next_action": "Continue.",
            }],
            "decisions": [],
        })
        child_events = [
            Delta(
                f"gen43-growth-{index}", "GEN-43", "requirement", "agent",
                {"requirement": f"Requirement {index}: " + "x" * 900},
                index, f"2026-08-29T22:{index:02d}:00Z",
            )
            for index in range(18)
        ]
        grown = deepcopy(graph)
        grown["child_comments"]["GEN-43"] = [
            {"id": f"gen43-growth-{index}",
             "body": encode_event_comment(event)}
            for index, event in enumerate(child_events)
        ]

        # This is the old root-only prospective surface. It fits the cap, so
        # only the child-aware production path can prevent the write.
        root_only = deepcopy(graph)
        root_only.pop("child_comments")
        root_preview, _ = load_material_history_for_projection_reconcile(
            root_only, client.comments, "GEN-37", manifest, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD, max_bytes=12 * 1024, max_items=500,
            relation_target_resolver=self.relation_target_resolver,
        )
        compact_context(
            root_preview, "GEN-37", max_bytes=12 * 1024, max_items=500,
            require_projection_authority=True,
        )

        route = dict(AUTHORITY)

        def invoke(responses):
            comments = mock.Mock()
            comments.comments.side_effect = lambda: [
                dict(item) for item in client.comments
            ]
            transport = mock.Mock()
            if callable(responses):
                transport.snapshot_for_root.side_effect = responses
            else:
                transport.snapshot_for_root.side_effect = [
                    deepcopy(item) for item in responses
                ]
            with tempfile.TemporaryDirectory() as directory:
                plan_path = Path(directory) / "plan.md"
                manifest_path = Path(directory) / "manifest.json"
                plan_path.write_bytes(raw)
                manifest_path.write_text(json.dumps(manifest))
                argv = [
                    "workstream_projection.py", "GEN-37", str(manifest_path),
                    "--remote-head", HEAD, "--plan-source", str(plan_path),
                    "--plan-identity", identity,
                    "--max-bytes", str(12 * 1024), "--max-items", "500",
                    "--apply", "--created-at", "2026-08-31T23:00:00Z",
                    "--expected-preview-sha256", "a" * 64,
                ]
                stdout, stderr = io.StringIO(), io.StringIO()
                with mock.patch.object(workstream_projection.sys, "argv", argv), \
                     mock.patch.object(workstream_projection.sys, "stdout", stdout), \
                     mock.patch.object(workstream_projection.sys, "stderr", stderr), \
                     mock.patch.object(workstream_projection, "load_linear_api_key", return_value="secret"), \
                     mock.patch.object(workstream_projection, "HttpGraphQLClient", return_value=client), \
                     mock.patch.object(workstream_projection, "resolve_linear_route", return_value=(route, None)), \
                     mock.patch.object(workstream_projection, "resolve_authenticated_issue_route", return_value=route), \
                     mock.patch.object(workstream_projection, "LinearGraphQLTransport", return_value=transport), \
                     mock.patch.object(workstream_projection, "LinearCommentEventAdapter", return_value=comments):
                    code = workstream_projection.main()
            return code, stderr.getvalue()

        writes_before = len(client.comments)
        code, error = invoke([grown, grown, grown, grown])
        self.assertEqual(code, 2)
        self.assertIn("resume_context_over_budget", error)
        self.assertEqual(len(client.comments), writes_before)

        # The preflight is stable and within budget, but the child grows before
        # the first append. The exact input frontier must refuse without a write.
        code, error = invoke([
            graph, graph, graph, graph, graph, graph, graph,
            grown, grown, grown,
        ])
        self.assertEqual(code, 2)
        self.assertIn(
            "projection_input_frontier_changed_reload_required", error,
        )
        self.assertEqual(len(client.comments), writes_before)

        # Linear cannot atomically compare child comments while creating a
        # root projection comment. If growth lands in that final gap, the
        # projection write may exist, but the operation must fail authority
        # closed instead of certifying a stale snapshot.
        live = {"graph": graph}
        original_execute = client.execute

        def interleaving_execute(query, variables):
            response = original_execute(query, variables)
            if "mutation WorkstreamDeltaCommentCreate" in query:
                live["graph"] = grown
            return response

        client.execute = interleaving_execute
        code, error = invoke(
            lambda *_args, **_kwargs: deepcopy(live["graph"])
        )
        self.assertEqual(code, 2)
        self.assertIn(
            "projection_input_frontier_changed_reload_required", error,
        )
        self.assertGreater(len(client.comments), writes_before)

    def test_projection_cli_seed_is_successful_partial_and_idempotent(self):
        client, adapter, source, graph, _children, manifest = (
            self.multi_terminal_repair_fixture(evidence_active=False)
        )
        graph = deepcopy(graph)
        graph["root"].update({
            "description": "Canonical plan: https://example.test/plan",
            "plan_revision": PLAN,
        })
        route = dict(AUTHORITY)
        comments = mock.Mock()
        comments.comments.side_effect = lambda: [
            dict(item) for item in client.comments
        ]
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = (
            live_graph_with_empty_child_comments(graph)
        )
        expected_writes = len(client.comments) + 2
        with tempfile.TemporaryDirectory() as directory:
            manifest_path = Path(directory) / "manifest.json"
            argv = [
                "workstream_projection.py", "GEN-37", str(manifest_path),
                "--remote-head", HEAD, "--plan-source", "ignored",
                "--plan-identity", source["identity"],
                "--max-bytes", "65536", "--max-items", "500",
                "--apply", "--created-at", "2026-08-31T23:00:00Z",
                "--expected-preview-sha256", "a" * 64,
            ]
            for _ in range(2):
                manifest_path.write_text(json.dumps(manifest))
                output = io.StringIO()
                with mock.patch.object(workstream_projection.sys, "argv", argv), \
                     mock.patch.object(workstream_projection.sys, "stdout", output), \
                     mock.patch.object(workstream_projection, "plan_payload", return_value={"source": source}), \
                     mock.patch.object(workstream_projection, "load_linear_api_key", return_value="secret"), \
                     mock.patch.object(workstream_projection, "HttpGraphQLClient", return_value=client), \
                     mock.patch.object(workstream_projection, "resolve_linear_route", return_value=(route, None)), \
                     mock.patch.object(workstream_projection, "resolve_authenticated_issue_route", return_value=route), \
                     mock.patch.object(workstream_projection, "LinearGraphQLTransport", return_value=transport), \
                     mock.patch.object(workstream_projection, "LinearCommentEventAdapter", return_value=comments):
                    self.assertEqual(workstream_projection.main(), 0)
                payload = json.loads(output.getvalue())
                self.assertFalse(payload["resume_authority_verified"])
                self.assertEqual(
                    payload["pending_terminal_closure"], ["GEN-70", "GEN-72"],
                )
                self.assertEqual(
                    payload["source_sync"]["resume_authority"],
                    "partial_terminal_closure_required",
                )
                self.assertEqual(len(client.comments), expected_writes)
                # Replay the exact originally reviewed manifest. A caller that
                # died before receiving the first receipt has no newer contract.
        self.assertEqual(len(json.loads(output.getvalue())["writes"]), 0)
        self.assertFalse(any(
            "issueCreate" in query or "issueUpdate" in query
            for query, _variables in client.calls
        ))

    def test_inactive_candidate_seed_cli_recovers_exact_crash_prefix(self):
        def case():
            client, current, source, graph, _children, manifest, _binding = (
                self.mixed_head_plan_generation_fixture(current_secondary=True)
            )
            predecessor = "b" * 64
            graph = deepcopy(graph)
            graph["root"].update({
                "description": f"Canonical plan: {source['identity']}",
                "plan_revision": predecessor,
            })
            route = dict(AUTHORITY)
            comments = mock.Mock()
            comments.comments.side_effect = lambda: [
                dict(item) for item in client.comments
            ]
            transport = mock.Mock()
            transport.snapshot_for_root.return_value = (
                live_graph_with_empty_child_comments(graph)
            )
            bound = workstream_projection.bind_projection_plan_generation(
                live_graph_with_empty_child_comments(graph), client.comments,
                workstream_id="GEN-37", requested_plan_revision=PLAN,
                authenticated_route=route,
            )
            bound = add_live_child_material_history(
                bound, authenticated_route=route,
                root_comments=client.comments,
                proposal_plan_revision=predecessor,
            )
            desired_scope = next(
                item["value"] for item in manifest["projection"]
                if (item["kind"], item["key"]) == ("scope", "root")
            )
            desired_contracts = {
                item["key"]: item["value"]
                for item in manifest["projection"]
                if item["kind"] == "evidence_contract"
            }
            binding, _authorities = (
                terminal_child_evidence_seed_predecessor_contract(
                    bound, current.state(), client.comments,
                    workstream_id="GEN-37",
                    predecessor_plan_revision=predecessor,
                    desired_scope=desired_scope,
                    seeds=manifest["terminal_child_evidence_seeds"],
                    desired_contracts=desired_contracts,
                )
            )
            manifest["terminal_child_evidence_seed_predecessor"] = binding
            directory = tempfile.TemporaryDirectory()
            self.addCleanup(directory.cleanup)
            manifest_path = Path(directory.name) / "manifest.json"
            argv = [
                "workstream_projection.py", "GEN-37", str(manifest_path),
                "--remote-head", HEAD, "--plan-source", "ignored",
                "--plan-identity", source["identity"],
                "--max-bytes", "65536", "--max-items", "500",
                "--apply", "--created-at", "2026-08-31T23:00:00Z",
                "--expected-preview-sha256", "a" * 64,
            ]

            def invoke(reviewed=manifest):
                manifest_path.write_text(json.dumps(reviewed))
                output, error = io.StringIO(), io.StringIO()
                with mock.patch.object(workstream_projection.sys, "argv", argv), \
                     mock.patch.object(workstream_projection.sys, "stdout", output), \
                     mock.patch.object(workstream_projection.sys, "stderr", error), \
                     mock.patch.object(workstream_projection, "plan_payload", return_value={"source": source}), \
                     mock.patch.object(workstream_projection, "load_linear_api_key", return_value="secret"), \
                     mock.patch.object(workstream_projection, "HttpGraphQLClient", return_value=client), \
                     mock.patch.object(workstream_projection, "resolve_linear_route", return_value=(route, None)), \
                     mock.patch.object(workstream_projection, "resolve_authenticated_issue_route", return_value=route), \
                     mock.patch.object(workstream_projection, "LinearGraphQLTransport", return_value=transport), \
                     mock.patch.object(workstream_projection, "LinearCommentEventAdapter", return_value=comments):
                    code = workstream_projection.main()
                return code, output.getvalue(), error.getvalue()

            return client, current, manifest, invoke

        def crash_after(client, invoke, append_limit):
            original_execute = client.execute
            append_count = 0

            def interrupted_execute(query, variables):
                nonlocal append_count
                response = original_execute(query, variables)
                if "commentCreate" in query:
                    append_count += 1
                    if append_count == append_limit:
                        raise SystemExit("simulated caller death")
                return response

            client.execute = interrupted_execute
            try:
                with self.assertRaisesRegex(
                    SystemExit, "simulated caller death",
                ):
                    invoke()
            finally:
                client.execute = original_execute

        client, current, manifest, invoke = case()
        crash_after(client, invoke, 3)
        self.assertEqual(current.state().revision, 3)
        code, output, error = invoke()
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(current.state().revision, 10)
        self.assertFalse(json.loads(output)["resume_authority_verified"])
        writes = len(client.comments)
        code, output, error = invoke()
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(len(client.comments), writes)
        self.assertEqual(json.loads(output)["writes"], [])

        def stale_source(reviewed):
            next(
                item for item in reviewed["projection"]
                if item["kind"] == "source"
            )["value"]["sha256"] = "c" * 64

        def stale_scope(reviewed):
            next(
                item for item in reviewed["projection"]
                if item["kind"] == "scope"
            )["value"]["repositories"][0]["exact_head"] = "f" * 40

        def stale_provenance(reviewed):
            next(
                item for item in reviewed["projection"]
                if item["kind"] == "provenance"
            )["value"]["session_id"] = "stale"

        def quarantine_drift(reviewed):
            reviewed["expected_projection_quarantine_count"] = 1
            reviewed["expected_projection_quarantine_sha256"] = "f" * 64

        for label, append_limit, mutate, expected_error in (
            (
                "stale_source", 3, stale_source,
                "generation_candidate_source_review_mismatch",
            ),
            (
                "stale_scope", 10, stale_scope,
                "terminal_child_evidence_seed_primary_head_transition_invalid",
            ),
            (
                "stale_provenance", 8, stale_provenance,
                "projection_review_stale_reload_required",
            ),
            (
                "quarantine", 3, quarantine_drift,
                "terminal_child_evidence_seed_scope_missing",
            ),
        ):
            with self.subTest(label=label):
                drift_client, _current, reviewed, drift_invoke = case()
                crash_after(drift_client, drift_invoke, append_limit)
                reviewed = deepcopy(reviewed)
                mutate(reviewed)
                writes_before = len(drift_client.comments)
                code, _output, error = drift_invoke(reviewed)
                self.assertEqual(code, 2)
                self.assertIn(expected_error, error)
                self.assertEqual(len(drift_client.comments), writes_before)

        for label, kind, key, value in (
            ("wrong_order", "source", "root", {
                "identity": "https://example.test/plan", "sha256": PLAN,
            }),
            ("unrelated", "provenance", "unexpected", {
                "agent": "codex", "machine": "M5", "session_id": "unexpected",
                "worktree": {"state": "safe", "head": HEAD},
            }),
        ):
            with self.subTest(label=label):
                drift_client, drift_current, reviewed, drift_invoke = case()
                crash_after(drift_client, drift_invoke, 3)
                drift_current.append(build_projection_event(
                    workstream_id="GEN-37", kind=kind, key=key, value=value,
                    plan_revision=PLAN,
                    expected_revision=drift_current.state().revision,
                    created_at="2026-08-29T23:00:00Z", authority=AUTHORITY,
                ))
                writes_before = len(drift_client.comments)
                code, _output, error = drift_invoke(reviewed)
                self.assertEqual(code, 2)
                self.assertIn(
                    "projection_review_stale_reload_required", error,
                )
                self.assertEqual(len(drift_client.comments), writes_before)

    def test_inactive_candidate_closure_cli_recovers_every_exact_crash_prefix(self):
        def case():
            client, current, source, graph, children, seed_manifest, _binding = (
                self.mixed_head_plan_generation_fixture(current_secondary=True)
            )
            predecessor = "b" * 64
            graph = deepcopy(graph)
            graph["root"].update({
                "description": f"Canonical plan: {source['identity']}",
                "plan_revision": predecessor,
            })
            route = dict(AUTHORITY)
            comments = mock.Mock()
            comments.comments.side_effect = lambda: [
                dict(item) for item in client.comments
            ]
            transport = mock.Mock()
            transport.snapshot_for_root.return_value = (
                live_graph_with_empty_child_comments(graph)
            )
            bound = workstream_projection.bind_projection_plan_generation(
                live_graph_with_empty_child_comments(graph), client.comments,
                workstream_id="GEN-37", requested_plan_revision=PLAN,
                authenticated_route=route,
            )
            bound = add_live_child_material_history(
                bound, authenticated_route=route,
                root_comments=client.comments,
                proposal_plan_revision=predecessor,
            )
            desired_scope = next(
                item["value"] for item in seed_manifest["projection"]
                if (item["kind"], item["key"]) == ("scope", "root")
            )
            desired_contracts = {
                item["key"]: item["value"]
                for item in seed_manifest["projection"]
                if item["kind"] == "evidence_contract"
            }
            binding, _authorities = (
                terminal_child_evidence_seed_predecessor_contract(
                    bound, current.state(), client.comments,
                    workstream_id="GEN-37",
                    predecessor_plan_revision=predecessor,
                    desired_scope=desired_scope,
                    seeds=seed_manifest["terminal_child_evidence_seeds"],
                    desired_contracts=desired_contracts,
                )
            )
            seed_manifest["terminal_child_evidence_seed_predecessor"] = binding
            directory = tempfile.TemporaryDirectory()
            self.addCleanup(directory.cleanup)
            manifest_path = Path(directory.name) / "manifest.json"
            argv = [
                "workstream_projection.py", "GEN-37", str(manifest_path),
                "--remote-head", HEAD, "--plan-source", "ignored",
                "--plan-identity", source["identity"],
                "--max-bytes", "65536", "--max-items", "500",
                "--apply", "--created-at", "2026-08-31T23:00:00Z",
                "--expected-preview-sha256", "a" * 64,
            ]

            def invoke(reviewed):
                manifest_path.write_text(json.dumps(reviewed))
                output, error = io.StringIO(), io.StringIO()
                with mock.patch.object(workstream_projection.sys, "argv", argv), \
                     mock.patch.object(workstream_projection.sys, "stdout", output), \
                     mock.patch.object(workstream_projection.sys, "stderr", error), \
                     mock.patch.object(workstream_projection, "plan_payload", return_value={"source": source}), \
                     mock.patch.object(workstream_projection, "load_linear_api_key", return_value="secret"), \
                     mock.patch.object(workstream_projection, "HttpGraphQLClient", return_value=client), \
                     mock.patch.object(workstream_projection, "resolve_linear_route", return_value=(route, None)), \
                     mock.patch.object(workstream_projection, "resolve_authenticated_issue_route", return_value=route), \
                     mock.patch.object(workstream_projection, "LinearGraphQLTransport", return_value=transport), \
                     mock.patch.object(workstream_projection, "LinearCommentEventAdapter", return_value=comments):
                    code = workstream_projection.main()
                return code, output.getvalue(), error.getvalue()

            code, _output, error = invoke(seed_manifest)
            self.assertEqual((code, error), (0, ""))
            self.assertEqual(current.state().revision, 10)
            active = workstream_projection._active_heads(current.state())
            projection = [
                {"kind": kind, "key": key, "value": deepcopy(event["value"])}
                for (kind, key), event in sorted(active.items())
                if kind != "disposition"
            ]
            repairs = []
            for child in sorted(children, key=lambda item: item["identifier"]):
                evidence = sorted(
                    [
                        {
                            "key": key,
                            "event_id": event["event_id"],
                            "value_sha256": canonical_digest(event["value"]),
                        }
                        for (kind, key), event in active.items()
                        if kind == "evidence_contract"
                        and event["value"].get("owning_child")
                        == child["identifier"]
                    ],
                    key=lambda item: (item["key"], item["event_id"]),
                )
                repairs.append({
                    "child_identifier": child["identifier"],
                    "child_issue_id": child["id"],
                    "expected_child_readback_sha256": canonical_digest(
                        terminal_child_readback(child)
                    ),
                    "expected_assignee_id": child["assignee"]["id"],
                    "approved_evidence_heads": evidence,
                })
            closure_manifest = {
                **reviewed_manifest(current, projection),
                "terminal_child_repairs": repairs,
            }
            return client, current, closure_manifest, invoke

        def crash_after(client, invoke, manifest, append_limit):
            original_execute = client.execute
            append_count = 0

            def interrupted_execute(query, variables):
                nonlocal append_count
                response = original_execute(query, variables)
                if "commentCreate" in query:
                    append_count += 1
                    if append_count == append_limit:
                        raise SystemExit("simulated caller death")
                return response

            client.execute = interrupted_execute
            try:
                try:
                    result = invoke(manifest)
                except SystemExit as error:
                    self.assertRegex(str(error), "simulated caller death")
                else:
                    self.fail(f"SystemExit not raised: {result!r}")
            finally:
                client.execute = original_execute

        # Every strict prefix of the six canonical closure writes resumes from
        # the exact originally reviewed manifest, then replays with zero writes.
        for prefix in range(1, 6):
            with self.subTest(prefix=prefix):
                client, current, manifest, invoke = case()
                crash_after(client, invoke, manifest, prefix)
                self.assertEqual(current.state().revision, 10 + prefix)
                code, _output, error = invoke(manifest)
                self.assertEqual((code, error), (0, ""))
                self.assertEqual(current.state().revision, 16)
                writes = len(client.comments)
                code, output, error = invoke(manifest)
                self.assertEqual((code, error), (0, ""))
                self.assertEqual(len(client.comments), writes)
                self.assertEqual(json.loads(output)["writes"], [])

        # Losing the response to the final closure append is also an exact
        # replay, not a stale-review error or a seventh write.
        client, current, manifest, invoke = case()
        crash_after(client, invoke, manifest, 6)
        self.assertEqual(current.state().revision, 16)
        writes = len(client.comments)
        code, output, error = invoke(manifest)
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(len(client.comments), writes)
        self.assertEqual(json.loads(output)["writes"], [])

        def source_drift(reviewed):
            next(item for item in reviewed["projection"]
                 if item["kind"] == "source")["value"]["sha256"] = "c" * 64

        def scope_drift(reviewed):
            next(item for item in reviewed["projection"]
                 if item["kind"] == "scope")["value"]["repositories"][0][
                     "exact_head"
                 ] = "f" * 40

        def provenance_drift(reviewed):
            next(item for item in reviewed["projection"]
                 if item["kind"] == "provenance")["value"]["session_id"] = "stale"

        def quarantine_drift(reviewed):
            reviewed["expected_projection_quarantine_count"] = 1
            reviewed["expected_projection_quarantine_sha256"] = "f" * 64

        def unrelated_drift(reviewed):
            reviewed["projection"].append({
                "kind": "provenance", "key": "unrelated",
                "value": {
                    "agent": "other", "machine": "M3",
                    "session_id": "unrelated",
                    "worktree": {"state": "safe", "head": HEAD},
                },
            })

        for label, mutate in (
            ("source", source_drift), ("scope", scope_drift),
            ("provenance", provenance_drift),
            ("quarantine", quarantine_drift),
            ("unrelated", unrelated_drift),
        ):
            with self.subTest(label=label):
                drift_client, _current, reviewed, drift_invoke = case()
                crash_after(drift_client, drift_invoke, reviewed, 3)
                reviewed = deepcopy(reviewed)
                mutate(reviewed)
                writes = len(drift_client.comments)
                code, _output, error = drift_invoke(reviewed)
                self.assertEqual(code, 2)
                self.assertTrue(error.startswith("workstream projection refused:"))
                self.assertEqual(len(drift_client.comments), writes)

        for label, kind, key, value in (
            ("order", "source", "root", {
                "identity": "https://example.test/plan", "sha256": PLAN,
            }),
            ("unrelated_live", "provenance", "unexpected", {
                "agent": "other", "machine": "M3",
                "session_id": "unexpected",
                "worktree": {"state": "safe", "head": HEAD},
            }),
        ):
            with self.subTest(label=label):
                drift_client, drift_current, reviewed, drift_invoke = case()
                crash_after(drift_client, drift_invoke, reviewed, 3)
                supersedes = None
                if (kind, key) in workstream_projection._active_heads(
                    drift_current.state()
                ):
                    supersedes = workstream_projection._active_heads(
                        drift_current.state()
                    )[(kind, key)]["event_id"]
                drift_current.append(build_projection_event(
                    workstream_id="GEN-37", kind=kind, key=key, value=value,
                    plan_revision=PLAN,
                    expected_revision=drift_current.state().revision,
                    created_at="2026-08-30T00:01:00Z", authority=AUTHORITY,
                    supersedes_event_id=supersedes,
                ))
                writes = len(drift_client.comments)
                code, _output, error = drift_invoke(reviewed)
                self.assertEqual(code, 2)
                self.assertIn("projection_review_stale_reload_required", error)
                self.assertEqual(len(drift_client.comments), writes)

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
