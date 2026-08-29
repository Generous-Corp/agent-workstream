#!/usr/bin/env python3
"""Append-only Linear projection for the complete workstream resume surface.

Each projection change is an immutable Linear comment.  Mutable current views
are derived by reducing the complete paginated comment stream; replacement of
a keyed value must name the exact event it supersedes.  This keeps scope,
relations, choices, evidence, provenance, and continuation disposition out of
unfenced issue-description overwrites.
"""

from __future__ import annotations

import base64
import binascii
from copy import deepcopy
import hashlib
import hmac
import json
import os
import re
import uuid
from dataclasses import dataclass
from typing import Any

from workstream_linear import (
    bootstrap_linear_route, GraphQLClient, HttpGraphQLClient, LinearTransportError,
    validate_issue_route,
)
from workstream_linear_events import COMMENT_CREATE_MUTATION, COMMENTS_QUERY


COMMENT_CREATE_CAPABILITY_QUERY = """
query WorkstreamProjectionCommentCreateCapability {
  __type(name: "CommentCreateInput") { inputFields { name } }
}
"""


PROJECTION_PREFIX = "<!-- workstream-projection:v1:"
PROJECTION_RE = re.compile(r"<!-- workstream-projection:v1:([A-Za-z0-9_-]+) -->")
KINDS = {
    "scope", "relation", "choice", "evidence_contract", "source",
    "provenance", "disposition", "closure_review", "lifecycle", "cas_activation",
    "quarantine_disposition", "child_closure",
}
SINGLETON_KINDS = {
    "scope", "source", "disposition", "lifecycle", "cas_activation",
    "quarantine_disposition",
}
TOMBSTONE = {"_projection_tombstone": True}
AUTHORITY_FIELDS = {"workspace_id", "team_id", "project_id", "root_issue_id"}
LEGACY_DIGEST_KIND_FULL_EVENTS = "canonical-full-events-v1"


class LinearProjectionError(LinearTransportError):
    """The remote projection cannot be persisted or reduced without guessing."""


def projection_slot_id(
    workstream_id: str, plan_revision: str, revision: int,
    authority: dict[str, str],
) -> str:
    """Return one UUIDv4-shaped remote create slot for a projection revision."""
    if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", workstream_id.upper()):
        raise LinearProjectionError("invalid_projection_workstream")
    if not isinstance(plan_revision, str) or not plan_revision:
        raise LinearProjectionError("projection_missing:plan_revision")
    if not isinstance(revision, int) or revision < 0:
        raise LinearProjectionError("invalid_projection_revision")
    validate_projection_authority(authority)
    material = _canonical([
        "workstream-projection-slot-v2", authority, workstream_id.upper(),
        plan_revision, revision,
    ])
    raw = bytearray(hashlib.sha256(material).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _immutable(event: dict[str, Any]) -> dict[str, Any]:
    return {key: deepcopy(value) for key, value in event.items() if key != "event_id"}


def _event_id(event: dict[str, Any]) -> str:
    return "wsp_" + hashlib.sha256(_canonical(_immutable(event))).hexdigest()[:32]


def _activation_digest_candidates(
    legacy_event_ids: list[str], accepted_legacy: list[dict[str, Any]],
) -> tuple[str, str]:
    return (
        hashlib.sha256(_canonical(sorted(legacy_event_ids))).hexdigest(),
        hashlib.sha256(_canonical(accepted_legacy)).hexdigest(),
    )


def _activation_legacy_digest_is_valid(
    value: dict[str, Any], accepted_legacy: list[dict[str, Any]],
) -> bool:
    historical_ids_digest, full_events_digest = _activation_digest_candidates(
        value["legacy_event_ids"], accepted_legacy,
    )
    observed_digest = value["legacy_events_sha256"]
    if "legacy_digest_kind" in value:
        return observed_digest == full_events_digest
    return sum((
        observed_digest == historical_ids_digest,
        observed_digest == full_events_digest,
    )) == 1


def validate_projection_authority(authority: dict[str, Any]) -> None:
    if not isinstance(authority, dict) or set(authority) != AUTHORITY_FIELDS:
        raise LinearProjectionError("invalid_projection_authority")
    if not all(isinstance(authority[field], str) and authority[field] for field in AUTHORITY_FIELDS):
        raise LinearProjectionError("invalid_projection_authority")


def build_projection_event(
    *, workstream_id: str, kind: str, key: str, value: dict[str, Any],
    plan_revision: str, expected_revision: int, created_at: str,
    supersedes_event_id: str | None = None, authority: dict[str, str] | None = None,
) -> dict[str, Any]:
    event = {
        "schema_version": 2,
        "workstream_id": workstream_id.upper(),
        "kind": kind,
        "key": key,
        "value": deepcopy(value),
        "plan_revision": plan_revision,
        "expected_revision": expected_revision,
        "created_at": created_at,
        "supersedes_event_id": supersedes_event_id,
        "authority": deepcopy(authority),
    }
    event["event_id"] = _event_id(event)
    validate_projection_event(event)
    return event


def validate_projection_event(event: dict[str, Any]) -> None:
    required = {
        "schema_version", "event_id", "workstream_id", "kind", "key",
        "value", "plan_revision", "expected_revision", "created_at",
        "supersedes_event_id",
    }
    schema_version = event.get("schema_version")
    if schema_version == 2:
        required.add("authority")
    if set(event) != required or schema_version not in {1, 2}:
        raise LinearProjectionError("invalid_projection_event_fields")
    if schema_version == 2:
        validate_projection_authority(event["authority"])
    if event.get("kind") not in KINDS:
        raise LinearProjectionError("invalid_projection_kind")
    for field in ("event_id", "workstream_id", "key", "plan_revision", "created_at"):
        if not isinstance(event.get(field), str) or not event[field].strip():
            raise LinearProjectionError(f"projection_missing:{field}")
    if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", event["workstream_id"]):
        raise LinearProjectionError("invalid_projection_workstream")
    if not isinstance(event.get("value"), dict):
        raise LinearProjectionError("invalid_projection_value")
    value = event["value"]
    tombstone = value == TOMBSTONE
    if event["kind"] == "cas_activation":
        historical_fields = {"legacy_event_ids", "legacy_events_sha256"}
        tagged_fields = {*historical_fields, "legacy_digest_kind"}
        fields = set(value)
        if tombstone or (fields != historical_fields and fields != tagged_fields):
            raise LinearProjectionError("invalid_projection_cas_activation")
        legacy_ids = value["legacy_event_ids"]
        if (
            event["key"] != "root"
            or not isinstance(legacy_ids, list)
            or len(legacy_ids) != len(set(legacy_ids))
            or not all(isinstance(item, str) and item for item in legacy_ids)
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(value["legacy_events_sha256"])
            )
        ):
            raise LinearProjectionError("invalid_projection_cas_activation")
        if fields == tagged_fields and (
            value["legacy_digest_kind"] != LEGACY_DIGEST_KIND_FULL_EVENTS
        ):
            raise LinearProjectionError("invalid_projection_cas_activation")
    if event["kind"] == "quarantine_disposition":
        required_disposition = {
            "event_ids", "events_sha256", "review_artifact_identity",
            "review_artifact_sha256", "reviewed_at",
        }
        event_ids = value.get("event_ids") if isinstance(value, dict) else None
        if (
            tombstone
            or set(value) != required_disposition
            or event["key"] != "root"
            or not isinstance(event_ids, list)
            or not event_ids
            or event_ids != sorted(set(event_ids))
            or not all(isinstance(item, str) and item for item in event_ids)
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("events_sha256", "")))
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(value.get("review_artifact_sha256", ""))
            )
            or not all(
                isinstance(value.get(field), str) and value[field]
                for field in ("review_artifact_identity", "reviewed_at")
            )
        ):
            raise LinearProjectionError("invalid_projection_quarantine_disposition")
    if event["kind"] == "source" and not tombstone:
        if not re.fullmatch(r"[0-9a-f]{64}", str(value.get("sha256", ""))):
            raise LinearProjectionError("invalid_projection_source_digest")
        if not any(isinstance(value.get(field), str) and value[field].strip()
                   for field in ("url", "identity")):
            raise LinearProjectionError("invalid_projection_source_identity")
    if event["kind"] == "provenance" and not tombstone:
        for field in ("agent", "machine", "session_id"):
            if not isinstance(value.get(field), str) or not value[field].strip():
                raise LinearProjectionError(f"invalid_projection_provenance:{field}")
    if event["kind"] == "closure_review" and not tombstone:
        review_fields_v1 = {
            "schema_version", "workstream_id", "snapshot_sha256",
            "closure_input_sha256", "repository_key", "exact_head", "verdict",
            "reviewer_agent", "reviewer_session_id", "implementer_session_id",
            "reviewed_at", "review_artifact_identity", "review_artifact_sha256",
            "trust_boundary", "procedural_independence",
        }
        review_fields_v2 = {
            "schema_version", "workstream_id", "snapshot_sha256",
            "closure_input_sha256", "repository_heads", "repository_truth_sha256",
            "verdict", "reviewer_agent", "reviewer_session_id",
            "implementer_session_id", "reviewed_at", "review_artifact_identity",
            "review_artifact_sha256", "trust_boundary", "procedural_independence",
        }
        review_version = value.get("schema_version")
        if not (
            (review_version == 1 and set(value) == review_fields_v1)
            or (review_version == 2 and set(value) == review_fields_v2)
        ):
            raise LinearProjectionError("invalid_projection_closure_review")
        if (
            event["key"] != value.get("snapshot_sha256")
            or value.get("workstream_id") != event["workstream_id"]
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("snapshot_sha256", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("closure_input_sha256", "")))
            or value.get("verdict") != "pass"
            or value.get("trust_boundary") != "shared_linear_credential"
            or value.get("procedural_independence") is not True
            or value.get("reviewer_session_id") == value.get("implementer_session_id")
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(value.get("review_artifact_sha256", ""))
            )
            or not all(isinstance(value.get(field), str) and value[field]
                       for field in ("reviewer_agent", "reviewer_session_id",
                                     "implementer_session_id", "reviewed_at",
                                     "review_artifact_identity"))
        ):
            raise LinearProjectionError("invalid_projection_closure_review")
        if review_version == 1 and (
            not isinstance(value.get("repository_key"), str)
            or not value["repository_key"]
            or not re.fullmatch(
                r"[0-9a-f]{40}(?:[0-9a-f]{24})?", str(value.get("exact_head", ""))
            )
        ):
            raise LinearProjectionError("invalid_projection_closure_review")
        if review_version == 2:
            heads = value.get("repository_heads")
            if (
                not isinstance(heads, dict) or len(heads) < 2
                or not all(isinstance(key, str) and key for key in heads)
                or not all(re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", str(head))
                           for head in heads.values())
                or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("repository_truth_sha256", "")))
            ):
                raise LinearProjectionError("invalid_projection_closure_review")
    if event["kind"] == "disposition" and not tombstone:
        if value.get("disposition") not in {"attach", "create_successor"}:
            raise LinearProjectionError("invalid_projection_disposition")
        if not isinstance(value.get("remote_head"), str) or not re.fullmatch(
            r"[0-9a-f]{40}(?:[0-9a-f]{24})?", value["remote_head"]
        ):
            raise LinearProjectionError("invalid_projection_disposition_head")
        if "recovered_from_checkpoint" not in value or (
            value["recovered_from_checkpoint"] is not None
            and not isinstance(value["recovered_from_checkpoint"], str)
        ):
            raise LinearProjectionError("invalid_projection_disposition_checkpoint")
    if event["kind"] == "lifecycle" and not tombstone:
        required_v1 = {
            "status", "github", "shipyard_receipt", "closure_input_sha256",
            "snapshot_sha256", "independent_review", "closure_receipt_sha256",
        }
        required_v2 = {
            "status", "repositories", "repository_truth_sha256",
            "closure_input_sha256", "snapshot_sha256", "independent_review",
            "closure_receipt_sha256",
        }
        if set(value) not in (required_v1, required_v2):
            raise LinearProjectionError("invalid_projection_lifecycle_fields")
        if value["status"] not in {"In Progress", "Landed — acceptance review required", "Done"}:
            raise LinearProjectionError("invalid_projection_lifecycle_status")
        if set(value) == required_v1 and (
            not isinstance(value["github"], dict)
            or not isinstance(value["shipyard_receipt"], dict)
        ):
            raise LinearProjectionError("invalid_projection_lifecycle:repositories")
        repository_truths = (
            [{"repository_key": value["shipyard_receipt"].get("repository_key"),
              "github": value["github"], "shipyard_receipt": value["shipyard_receipt"]}]
            if set(value) == required_v1 else value["repositories"]
        )
        if (
            not isinstance(repository_truths, list)
            or len(repository_truths) < (1 if set(value) == required_v1 else 2)
        ):
            raise LinearProjectionError("invalid_projection_lifecycle:repositories")
        seen_repository_keys: set[str] = set()
        for truth in repository_truths:
            if not isinstance(truth, dict) or set(truth) != {
                "repository_key", "github", "shipyard_receipt",
            }:
                raise LinearProjectionError("invalid_projection_lifecycle:repositories")
            repository_key_value = truth["repository_key"]
            if not isinstance(repository_key_value, str) or not repository_key_value or repository_key_value in seen_repository_keys:
                raise LinearProjectionError("invalid_projection_lifecycle:repositories")
            seen_repository_keys.add(repository_key_value)
            github = truth["github"]
            shipyard = truth["shipyard_receipt"]
            if not isinstance(github, dict) or set(github) != {
            "repository", "provider_repository_id", "pr_number", "pr_head",
            "merged", "merge_sha",
            }:
                raise LinearProjectionError("invalid_projection_lifecycle:github")
            if (
                not re.fullmatch(r"[a-z0-9_.-]+/[a-z0-9_.-]+", str(github["repository"]))
                or not isinstance(github["provider_repository_id"], str)
                or not github["provider_repository_id"]
                or not isinstance(github["pr_number"], int) or github["pr_number"] <= 0
                or not re.fullmatch(r"[0-9a-f]{40}", str(github["pr_head"]))
                or github["merged"] is not True
                or not re.fullmatch(r"[0-9a-f]{40}", str(github["merge_sha"]))
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:github")
            if not isinstance(shipyard, dict) or set(shipyard) != {
            "schema_version", "repository", "repository_key", "pr_number", "head",
            "disposition", "receipt_id", "receipt_sha256",
            } or shipyard.get("schema_version") != 1:
                raise LinearProjectionError("invalid_projection_lifecycle:shipyard_receipt")
            shipyard_digest = hashlib.sha256(_canonical({
                key: item for key, item in shipyard.items() if key != "receipt_sha256"
            })).hexdigest()
            if (
                shipyard.get("repository") != github["repository"]
                or shipyard.get("repository_key") != repository_key_value
                or shipyard.get("pr_number") != github["pr_number"]
                or shipyard.get("head") != github["pr_head"]
                or shipyard.get("disposition") not in {"merged", "already_merged", "landed"}
                or not isinstance(shipyard.get("receipt_id"), str) or not shipyard["receipt_id"]
                or shipyard.get("receipt_sha256") != shipyard_digest
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:shipyard_receipt")
        if set(value) == required_v2 and value.get("repository_truth_sha256") != hashlib.sha256(
            _canonical(repository_truths)
        ).hexdigest():
            raise LinearProjectionError("invalid_projection_lifecycle:repository_truth_sha256")
        if not re.fullmatch(r"[0-9a-f]{64}", str(value["closure_input_sha256"])):
            raise LinearProjectionError("invalid_projection_lifecycle:closure_input_sha256")
        if not re.fullmatch(r"[0-9a-f]{64}", str(value["snapshot_sha256"])):
            raise LinearProjectionError("invalid_projection_lifecycle:snapshot_sha256")
        if value["status"] == "Done":
            if not isinstance(value["independent_review"], dict):
                raise LinearProjectionError("done_requires_independent_review")
            review = value["independent_review"]
            review_v1 = {
                "schema_version", "workstream_id", "snapshot_sha256",
                "closure_input_sha256", "repository_key", "exact_head", "verdict",
                "reviewer_agent", "reviewer_session_id", "implementer_session_id",
                "reviewed_at", "review_artifact_identity", "review_artifact_sha256",
                "trust_boundary", "procedural_independence",
            }
            review_v2 = {
                "schema_version", "workstream_id", "snapshot_sha256",
                "closure_input_sha256", "repository_heads", "repository_truth_sha256",
                "verdict", "reviewer_agent", "reviewer_session_id",
                "implementer_session_id", "reviewed_at", "review_artifact_identity",
                "review_artifact_sha256", "trust_boundary", "procedural_independence",
            }
            aggregate_heads = {
                truth["repository_key"]: truth["github"]["pr_head"]
                for truth in repository_truths
            }
            if not (
                (set(value) == required_v1 and set(review) == review_v1 and review.get("schema_version") == 1)
                or (set(value) == required_v2 and set(review) == review_v2 and review.get("schema_version") == 2)
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:independent_review")
            if (
                review.get("workstream_id") != event["workstream_id"]
                or review.get("snapshot_sha256") != value["snapshot_sha256"]
                or review.get("closure_input_sha256") != value["closure_input_sha256"]
                or review.get("verdict") != "pass"
                or review.get("trust_boundary") != "shared_linear_credential"
                or review.get("procedural_independence") is not True
                or review.get("reviewer_session_id") == review.get("implementer_session_id")
                or not re.fullmatch(r"[0-9a-f]{64}", str(review.get("snapshot_sha256", "")))
                or not re.fullmatch(
                    r"[0-9a-f]{64}", str(review.get("review_artifact_sha256", ""))
                )
                or not all(isinstance(review.get(field), str) and review[field]
                           for field in ("reviewer_agent", "reviewer_session_id",
                                         "implementer_session_id", "reviewed_at",
                                         "review_artifact_identity"))
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:independent_review")
            if set(value) == required_v1 and (
                review.get("repository_key") != repository_truths[0]["repository_key"]
                or review.get("exact_head") != repository_truths[0]["github"]["pr_head"]
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:independent_review")
            if set(value) == required_v2 and (
                review.get("repository_heads") != aggregate_heads
                or review.get("repository_truth_sha256") != value["repository_truth_sha256"]
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:independent_review")
            if not re.fullmatch(r"[0-9a-f]{64}", str(value["closure_receipt_sha256"])):
                raise LinearProjectionError("done_requires_closure_receipt")
        elif value["independent_review"] is not None or value["closure_receipt_sha256"] is not None:
            raise LinearProjectionError("non_done_lifecycle_has_closure_receipt")
    if event["kind"] == "choice" and not tombstone and value.get("event_id") != event["key"]:
        raise LinearProjectionError("projection_choice_key_mismatch")
    if event["kind"] == "evidence_contract" and not tombstone:
        if value.get("slice_id") != event["key"]:
            raise LinearProjectionError("projection_evidence_key_mismatch")
        if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(value.get("owning_child", ""))):
            raise LinearProjectionError("projection_evidence_owner_invalid")
    if event["kind"] == "child_closure" and not tombstone:
        required_closure = {
            "schema_version", "child_identifier", "child_issue_id",
            "parent_issue_id", "workspace_id", "team_id", "project_id",
            "assignee_id", "state_id", "state_name", "state_type",
            "plan_revision", "repository_key", "exact_head",
            "evidence_heads", "evidence_receipts_sha256",
            "child_readback_sha256",
        }
        evidence_heads = value.get("evidence_heads")
        valid_evidence_heads = (
            isinstance(evidence_heads, list)
            and bool(evidence_heads)
            and all(
                isinstance(item, dict)
                and set(item) == {"key", "event_id", "value_sha256"}
                and all(isinstance(item.get(field), str) and item[field]
                        for field in ("key", "event_id"))
                and re.fullmatch(r"[0-9a-f]{64}", str(item.get("value_sha256", "")))
                for item in evidence_heads
            )
        )
        if (
            set(value) != required_closure
            or value.get("schema_version") not in {1, 2}
            or event["key"] != value.get("child_identifier")
            or value.get("plan_revision") != event["plan_revision"]
            or value.get("state_type") != "completed"
            or not valid_evidence_heads
            or evidence_heads != sorted(
                evidence_heads, key=lambda item: (item.get("key", ""), item.get("event_id", ""))
            )
            or not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", str(value.get("exact_head", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("evidence_receipts_sha256", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("child_readback_sha256", "")))
            or not all(
                isinstance(value.get(field), str) and value[field]
                for field in (
                    "child_identifier", "child_issue_id", "parent_issue_id",
                    "workspace_id", "team_id", "project_id",
                    "state_id", "state_name", "repository_key",
                )
            )
            or (
                value.get("schema_version") == 1
                and not (
                    isinstance(value.get("assignee_id"), str)
                    and bool(value["assignee_id"])
                )
            )
            or (
                value.get("schema_version") == 2
                and not (
                    value.get("assignee_id") is None
                    or (
                        isinstance(value.get("assignee_id"), str)
                        and bool(value["assignee_id"])
                    )
                )
            )
        ):
            raise LinearProjectionError("invalid_projection_child_closure")
    revision = event.get("expected_revision")
    if not isinstance(revision, int) or revision < 0:
        raise LinearProjectionError("invalid_projection_revision")
    supersedes = event.get("supersedes_event_id")
    if supersedes is not None and (not isinstance(supersedes, str) or not supersedes):
        raise LinearProjectionError("invalid_projection_supersedes")
    if event.get("event_id") != _event_id(event):
        raise LinearProjectionError("projection_event_id_mismatch")


def encode_projection_comment(event: dict[str, Any]) -> str:
    validate_projection_event(event)
    material = _canonical(event)
    envelope = {
        "event": event,
        "sha256": hashlib.sha256(material).hexdigest(),
    }
    encoded = base64.urlsafe_b64encode(_canonical(envelope)).decode("ascii").rstrip("=")
    return f"{PROJECTION_PREFIX}{encoded} -->"


def _decode_projection(encoded: str) -> dict[str, Any]:
    try:
        envelope = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        if set(envelope) != {"event", "sha256"}:
            raise ValueError("unexpected envelope fields")
        event = envelope["event"]
        digest = envelope["sha256"]
        if not isinstance(event, dict) or not isinstance(digest, str):
            raise ValueError("invalid envelope")
        if not hmac.compare_digest(digest, hashlib.sha256(_canonical(event)).hexdigest()):
            raise ValueError("digest mismatch")
        validate_projection_event(event)
        return event
    except (
        binascii.Error, json.JSONDecodeError, KeyError, TypeError, ValueError,
        LinearProjectionError,
    ) as error:
        raise LinearProjectionError("malformed_projection_marker") from error


@dataclass(frozen=True)
class ReducedProjection:
    workstream_id: str
    revision: int
    events: tuple[dict[str, Any], ...]
    remote_ids: dict[str, str]
    snapshot: dict[str, Any]


def reduce_projection_comments(
    comments: list[dict[str, Any]], *, workstream_id: str,
    expected_plan_revision: str,
    authenticated_route: dict[str, str] | None = None,
    authenticated_source: dict[str, Any] | None = None,
) -> ReducedProjection:
    observed: dict[str, tuple[dict[str, Any], str, bytes]] = {}
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str):
            raise LinearProjectionError("malformed_projection_marker")
        if PROJECTION_PREFIX not in body:
            continue
        matches = PROJECTION_RE.findall(body)
        if len(matches) != 1 or body.count(PROJECTION_PREFIX) != 1:
            raise LinearProjectionError("malformed_projection_marker")
        event = _decode_projection(matches[0])
        if event["workstream_id"] != workstream_id:
            raise LinearProjectionError("workstream_id_mismatch")
        signature = _canonical(event)
        previous = observed.get(event["event_id"])
        if previous:
            reason = "duplicate_projection_event_id" if previous[2] == signature else "conflicting_projection_event_id"
            raise LinearProjectionError(f"{reason}:{event['event_id']}")
        remote_id = comment.get("id")
        if not isinstance(remote_id, str) or not remote_id:
            raise LinearProjectionError("projection_comment_missing_remote_id")
        observed[event["event_id"]] = (event, remote_id, signature)

    history = sorted(
        (item[0] for item in observed.values()),
        key=lambda item: (
            item["plan_revision"], item["expected_revision"],
            item["created_at"], item["event_id"],
        ),
    )
    generation = [
        event for event in history if event["plan_revision"] == expected_plan_revision
    ]
    stale_events = [
        event for event in history if event["plan_revision"] != expected_plan_revision
    ]
    legacy = sorted(
        (event for event in generation if event["schema_version"] == 1),
        key=lambda item: (item["expected_revision"], item["created_at"], item["event_id"]),
    )
    modern = sorted(
        (event for event in generation if event["schema_version"] == 2),
        key=lambda item: (item["expected_revision"], item["created_at"], item["event_id"]),
    )
    quarantined: list[dict[str, Any]] = []
    accepted_legacy = legacy
    if modern:
        activation = modern[0]
        if activation["expected_revision"] == 0:
            if activation["kind"] == "cas_activation":
                raise LinearProjectionError(
                    "projection_v2_activation_without_legacy"
                )
            accepted_legacy = []
            quarantined = legacy
        else:
            if activation["kind"] != "cas_activation":
                raise LinearProjectionError("projection_v2_activation_required")
            reviewed_ids = activation["value"]["legacy_event_ids"]
            by_id = {event["event_id"]: event for event in legacy}
            if len(reviewed_ids) != activation["expected_revision"] or any(
                event_id not in by_id for event_id in reviewed_ids
            ):
                raise LinearProjectionError("projection_v2_activation_legacy_mismatch")
            accepted_legacy = sorted(
                (by_id[event_id] for event_id in reviewed_ids),
                key=lambda item: (
                    item["expected_revision"], item["created_at"], item["event_id"],
                ),
            )
            if (
                "legacy_digest_kind" in activation["value"]
                and reviewed_ids
                != [event["event_id"] for event in accepted_legacy]
            ):
                raise LinearProjectionError(
                    "projection_v2_activation_legacy_order_mismatch"
                )
            reviewed = set(reviewed_ids)
            quarantined = [event for event in legacy if event["event_id"] not in reviewed]
            if not _activation_legacy_digest_is_valid(
                activation["value"], accepted_legacy,
            ):
                raise LinearProjectionError("projection_v2_activation_legacy_digest_mismatch")
    for index, event in enumerate(accepted_legacy):
        if event["expected_revision"] > index:
            raise LinearProjectionError(
                f"projection_revision_ahead:{event['event_id']}:{event['expected_revision']}:{index}"
            )
    for offset, event in enumerate(modern):
        index = len(accepted_legacy) + offset
        if event["expected_revision"] != index:
            raise LinearProjectionError(
                f"projection_revision_mismatch:{event['event_id']}:{event['expected_revision']}:{index}"
            )
        authority = event["authority"]
        if authenticated_route is not None:
            for field in AUTHORITY_FIELDS:
                if authority[field] != authenticated_route.get(field):
                    raise LinearProjectionError(f"projection_route_mismatch:{field}")
        if observed[event["event_id"]][1] != projection_slot_id(
            workstream_id, event["plan_revision"], index, authority,
        ):
            raise LinearProjectionError(f"projection_slot_identity_mismatch:{event['event_id']}")
    events = [*accepted_legacy, *modern]
    active: dict[tuple[str, str], dict[str, Any]] = {}
    heads: dict[tuple[str, str], dict[str, Any]] = {}
    for index, event in enumerate(events):
        identity = (event["kind"], event["key"])
        current = heads.get(identity)
        supersedes = event["supersedes_event_id"]
        if current is None and supersedes is not None:
            raise LinearProjectionError(f"projection_supersedes_missing:{event['event_id']}")
        if current is not None and supersedes != current["event_id"]:
            raise LinearProjectionError(f"projection_concurrent_conflict:{event['kind']}:{event['key']}")
        heads[identity] = event
        if event["value"] == TOMBSTONE:
            active.pop(identity, None)
        else:
            active[identity] = event

    by_kind: dict[str, list[dict[str, Any]]] = {kind: [] for kind in KINDS}
    for (kind, _key), event in active.items():
        by_kind[kind].append(deepcopy(event["value"]))
    for kind in by_kind:
        by_kind[kind].sort(key=lambda value: _canonical(value))
    for kind in SINGLETON_KINDS:
        if len(by_kind[kind]) > 1:
            raise LinearProjectionError(f"multiple_projection_singletons:{kind}")
    source = by_kind["source"][0] if by_kind["source"] else None
    if source is not None and source.get("sha256") != expected_plan_revision:
        raise LinearProjectionError("projection_source_plan_mismatch")
    if authenticated_source is not None and source is not None:
        source_identity = source.get("identity") or source.get("url")
        if source_identity != authenticated_source.get("identity"):
            raise LinearProjectionError("projection_source_identity_mismatch")
        if source.get("sha256") != authenticated_source.get("sha256"):
            raise LinearProjectionError("projection_source_bytes_mismatch")
    scope = by_kind["scope"][0] if by_kind["scope"] else None
    if authenticated_route is not None and scope is not None:
        linear = scope.get("linear") or {}
        for field in ("workspace_id", "team_id", "project_id", "root_issue_id"):
            if linear.get(field) != authenticated_route.get(field):
                raise LinearProjectionError(f"projection_route_mismatch:{field}")

    quarantine_disposition = (
        by_kind["quarantine_disposition"][0]
        if by_kind["quarantine_disposition"] else None
    )
    retired_quarantine_ids: set[str] = set()
    if quarantine_disposition is not None:
        retired_quarantine_ids = set(quarantine_disposition["event_ids"])
        quarantined_by_id = {event["event_id"]: event for event in quarantined}
        if not retired_quarantine_ids.issubset(quarantined_by_id):
            raise LinearProjectionError("quarantine_disposition_unknown_event")
        reviewed_events = [
            quarantined_by_id[event_id]
            for event_id in sorted(retired_quarantine_ids)
        ]
        if quarantine_disposition["events_sha256"] != hashlib.sha256(
            _canonical(reviewed_events)
        ).hexdigest():
            raise LinearProjectionError("quarantine_disposition_digest_mismatch")
    unresolved_quarantine = [
        event for event in quarantined
        if event["event_id"] not in retired_quarantine_ids
    ]

    snapshot = {
        "scope": scope,
        "relations": by_kind["relation"],
        "choice_events": by_kind["choice"],
        "evidence_contracts": by_kind["evidence_contract"],
        "child_closures": by_kind["child_closure"],
        "source": source,
        "provenance": by_kind["provenance"],
        "closure_reviews": by_kind["closure_review"],
        "disposition": by_kind["disposition"][0] if by_kind["disposition"] else None,
        "lifecycle": by_kind["lifecycle"][0] if by_kind["lifecycle"] else None,
        "quarantine_disposition": quarantine_disposition,
        "projection_events": [deepcopy(event) for event in events],
        "projection_history": [deepcopy(event) for event in stale_events],
        "projection_quarantined": [deepcopy(event) for event in quarantined],
        "projection_unresolved_quarantine": [
            deepcopy(event) for event in unresolved_quarantine
        ],
        "projection_revision": len(events),
        "projection_recovery": {
            "state": (
                "current" if any(by_kind.values())
                else "stale_plan" if stale_events
                else "not_found"
            ),
            "stale_plan_count": len(stale_events),
        },
    }
    return ReducedProjection(
        workstream_id=workstream_id, revision=len(events), events=tuple(events),
        remote_ids={event_id: item[1] for event_id, item in observed.items()},
        snapshot=snapshot,
    )


class LinearProjectionAdapter:
    supports_atomic_cas = False
    supports_append_only_events = True

    def __init__(
        self, client: GraphQLClient, *, issue_id: str, workstream_id: str,
        plan_revision: str, workspace_id: str | None = None,
        team_id: str | None = None, project_id: str | None = None,
        root_issue_id: str | None = None,
    ):
        self.client = client
        self.issue_id = issue_id
        self.workstream_id = workstream_id.upper()
        self.plan_revision = plan_revision
        self.workspace_id = workspace_id
        self.team_id = team_id
        self.project_id = project_id
        self.root_issue_id = root_issue_id
        self._comment_id_capability_verified = False
        if any((workspace_id, team_id, project_id, root_issue_id)) and not all(
            (workspace_id, team_id, project_id, root_issue_id)
        ):
            raise ValueError("Linear workspace, team, project, and root issue IDs must be supplied together")

    @property
    def authority(self) -> dict[str, str]:
        authority = {
            "workspace_id": self.workspace_id,
            "team_id": self.team_id,
            "project_id": self.project_id,
            "root_issue_id": self.root_issue_id,
        }
        validate_projection_authority(authority)
        return authority  # type: ignore[return-value]

    def _assert_comment_id_capability(self) -> None:
        if self._comment_id_capability_verified:
            return
        result = self.client.execute(COMMENT_CREATE_CAPABILITY_QUERY, {})
        fields = ((result.get("__type") or {}).get("inputFields") or [])
        if not isinstance(fields, list) or "id" not in {
            field.get("name") for field in fields if isinstance(field, dict)
        }:
            raise LinearProjectionError("linear_comment_create_id_capability_unavailable")
        self._comment_id_capability_verified = True

    def activate_v2(
        self, *, created_at: str, expected_revision: int | None = None,
        expected_legacy_event_ids: list[str] | None = None,
        expected_legacy_events_sha256: str | None = None,
    ) -> dict[str, Any] | None:
        """Fence reviewed v1 history before accepting any v2 CAS writes."""
        before = self.state()
        if any(event["schema_version"] == 2 for event in before.events) or not before.events:
            return None
        legacy_ids = [event["event_id"] for event in before.events]
        legacy_events_sha256 = hashlib.sha256(
            _canonical(list(before.events))
        ).hexdigest()
        if (
            expected_revision is not None and before.revision != expected_revision
        ) or (
            expected_legacy_event_ids is not None
            and legacy_ids != expected_legacy_event_ids
        ) or (
            expected_legacy_events_sha256 is not None
            and legacy_events_sha256 != expected_legacy_events_sha256
        ):
            raise LinearProjectionError("projection_v2_activation_stale_reload_required")
        event = build_projection_event(
            workstream_id=self.workstream_id, kind="cas_activation", key="root",
            value={
                "legacy_digest_kind": LEGACY_DIGEST_KIND_FULL_EVENTS,
                "legacy_event_ids": legacy_ids,
                "legacy_events_sha256": legacy_events_sha256,
            },
            plan_revision=self.plan_revision, expected_revision=before.revision,
            created_at=created_at, authority=self.authority,
        )
        return self.append(event)

    @classmethod
    def from_env(
        cls, *, issue_id: str, workstream_id: str, plan_revision: str,
        env: dict[str, str] | None = None, config_path: str | None = None,
    ) -> "LinearProjectionAdapter":
        from workstream_config import load_linear_api_key, resolve_linear_route

        values = os.environ if env is None else env
        token = load_linear_api_key(env=values)
        if not token:
            raise LinearProjectionError("linear_auth_unavailable")
        client = HttpGraphQLClient(token)
        route, _resolved = resolve_linear_route(config_path=config_path, env=values)
        if not route:
            route = bootstrap_linear_route(client, workstream_id)
        return cls(
            client, issue_id=issue_id, workstream_id=workstream_id,
            plan_revision=plan_revision, workspace_id=route.get("workspace_id"),
            team_id=route.get("team_id"), project_id=route.get("project_id"),
            root_issue_id=route.get("root_issue_id"),
        )

    def _comments(self) -> list[dict[str, Any]]:
        comments: list[dict[str, Any]] = []
        after: str | None = None
        seen: set[str] = set()
        while True:
            result = self.client.execute(COMMENTS_QUERY, {"issueId": self.issue_id, "after": after})
            issue = result.get("issue")
            if not issue or issue.get("identifier") != self.workstream_id:
                raise LinearProjectionError("Linear workstream issue not found or mismatched")
            if self.root_issue_id and issue.get("id") != self.root_issue_id:
                raise LinearProjectionError("projection_route_mismatch:root_issue_id")
            validate_issue_route(
                issue, workspace_id=self.workspace_id, team_id=self.team_id,
                project_id=self.project_id,
            )
            connection = issue.get("comments") or {}
            nodes = connection.get("nodes")
            page_info = connection.get("pageInfo")
            if not isinstance(nodes, list) or not isinstance(page_info, dict):
                raise LinearProjectionError("invalid Linear comment connection")
            comments.extend(nodes)
            if not page_info.get("hasNextPage"):
                return comments
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after or after in seen:
                raise LinearProjectionError("invalid Linear comment pagination cursor")
            seen.add(after)

    def state(self) -> ReducedProjection:
        return reduce_projection_comments(
            self._comments(), workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route={
                "workspace_id": self.workspace_id,
                "team_id": self.team_id,
                "project_id": self.project_id,
                "root_issue_id": self.root_issue_id,
            } if all((self.workspace_id, self.team_id, self.project_id, self.root_issue_id)) else None,
        )

    def append(
        self, event: dict[str, Any], *,
        expected_quarantine_count: int | None = None,
        expected_quarantine_sha256: str | None = None,
    ) -> dict[str, Any]:
        validate_projection_event(event)
        if (expected_quarantine_count is None) != (
            expected_quarantine_sha256 is None
        ):
            raise LinearProjectionError("projection_quarantine_fence_incomplete")
        if event["workstream_id"] != self.workstream_id or event["plan_revision"] != self.plan_revision:
            raise LinearProjectionError("projection_route_or_plan_mismatch")
        before = self.state()
        if expected_quarantine_count is not None or expected_quarantine_sha256 is not None:
            quarantine = before.snapshot.get("projection_quarantined") or []
            if (
                len(quarantine) != expected_quarantine_count
                or hashlib.sha256(_canonical(quarantine)).hexdigest()
                != expected_quarantine_sha256
            ):
                raise LinearProjectionError(
                    "projection_quarantine_changed_reload_required"
                )
        existing_id = before.remote_ids.get(event["event_id"])
        if existing_id:
            existing = next(item for item in before.events if item["event_id"] == event["event_id"])
            if existing != event:
                raise LinearProjectionError(f"conflicting_projection_event_id:{event['event_id']}")
            return {"event_id": event["event_id"], "remote_id": existing_id, "revision": before.revision}
        if event["expected_revision"] != before.revision:
            raise LinearProjectionError("projection_slot_stale_reload_required")
        if event["schema_version"] != 2 or event.get("authority") != self.authority:
            raise LinearProjectionError("projection_append_authority_mismatch")
        if (
            any(item["schema_version"] == 1 for item in before.events)
            and not any(item["schema_version"] == 2 for item in before.events)
            and event["kind"] != "cas_activation"
        ):
            raise LinearProjectionError("projection_v2_activation_required")
        current = next(
            (
                item for item in reversed(before.events)
                if item["kind"] == event["kind"] and item["key"] == event["key"]
            ),
            None,
        )
        if current is None and event["supersedes_event_id"] is not None:
            raise LinearProjectionError("projection_supersedes_missing")
        if current is not None and event["supersedes_event_id"] != current["event_id"]:
            raise LinearProjectionError(
                f"projection_concurrent_conflict:{event['kind']}:{event['key']}"
            )
        slot_id = projection_slot_id(
            self.workstream_id, self.plan_revision, event["expected_revision"],
            self.authority,
        )
        self._assert_comment_id_capability()
        try:
            response = self.client.execute(
                COMMENT_CREATE_MUTATION,
                {"input": {
                    "id": slot_id, "issueId": self.issue_id,
                    "body": encode_projection_comment(event),
                }},
            )
        except LinearTransportError:
            # A deterministic create-ID collision is the remote CAS loser path.
            # Reload before deciding whether this is identical replay or a
            # conflicting winner; an unavailable reload preserves the original
            # transport failure and never attempts another write.
            after_error = self.state()
            winner = next(
                (item for item in after_error.events
                 if after_error.remote_ids.get(item["event_id"]) == slot_id),
                None,
            )
            if winner == event:
                return {
                    "event_id": event["event_id"], "remote_id": slot_id,
                    "revision": after_error.revision,
                }
            if winner is not None:
                raise LinearProjectionError("projection_slot_lost_reload_required")
            raise
        created = response.get("commentCreate") or {}
        comment = created.get("comment")
        if (created.get("success") is not True or not comment
                or comment.get("id") != slot_id):
            raise LinearProjectionError("Linear comment creation returned no durable receipt")
        after = self.state()
        if expected_quarantine_count is not None or expected_quarantine_sha256 is not None:
            quarantine = after.snapshot.get("projection_quarantined") or []
            if (
                len(quarantine) != expected_quarantine_count
                or hashlib.sha256(_canonical(quarantine)).hexdigest()
                != expected_quarantine_sha256
            ):
                raise LinearProjectionError(
                    "projection_quarantine_changed_reload_required"
                )
        if after.remote_ids.get(event["event_id"]) != comment["id"]:
            raise LinearProjectionError("projection_append_not_observed")
        return {"event_id": event["event_id"], "remote_id": comment["id"], "revision": after.revision}
