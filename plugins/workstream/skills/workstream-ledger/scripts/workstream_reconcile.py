#!/usr/bin/env python3
"""Reconcile live landing truth and persist one revision-fenced lifecycle value."""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import time
from typing import Any, Callable
import urllib.error
import urllib.request

from workstream_closure import review as closure_review
from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_http import default_ssl_context
from workstream_linear import (
    HttpGraphQLClient, LinearGraphQLTransport, LinearTransportError,
    parse_plan_revision, resolve_authenticated_issue_route,
)
from workstream_linear_events import LinearCommentEventAdapter
from workstream_linear_projection import (
    build_projection_event, LinearProjectionAdapter, LinearProjectionError,
    reduce_projection_comments, TOMBSTONE,
)
from workstream_plan import plan_payload
from workstream_projection import stable_live_readback
from workstream_resume import (
    add_material_history, closure_snapshot_digest, extract_token, ResumeError,
)
from workstream_scope import relation_target_key, repository_key


OID = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"[0-9a-f]{64}")
MAX_PROVIDER_BYTES = 1024 * 1024
MAX_TOKEN_BYTES = 8192
SAFE_COMMAND_ENV = {
    "PATH", "HOME", "USERPROFILE", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT",
    "TMP", "TEMP", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE",
}
RELATION_TARGET_QUERY = """
query WorkstreamRelationTarget($issueId: String!) {
  issue(id: $issueId) {
    id identifier description
    team { id organization { id } }
    project { id }
  }
}
"""


class ReconcileError(RuntimeError):
    """Live truth or closure input was incomplete, stale, or contradictory."""


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


class GitHubTruthReader:
    def __init__(
        self, token: str, *, api_base: str = "https://api.github.com",
        timeout: float = 20.0,
        opener: Callable[..., Any] = urllib.request.urlopen,
    ):
        if not token:
            raise ReconcileError("github_auth_unavailable")
        self.token = token
        if api_base.rstrip("/") != "https://api.github.com":
            raise ReconcileError("github_api_authority_must_be_api_github_com")
        self.api_base = "https://api.github.com"
        self.timeout = timeout
        self.opener = opener

    def read(
        self, *, repository: str, provider_repository_id: str,
        pr_number: int, expected_head: str,
    ) -> dict[str, Any]:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ReconcileError("invalid_github_repository")
        if not isinstance(pr_number, int) or pr_number <= 0:
            raise ReconcileError("invalid_github_pr")
        if not OID.fullmatch(expected_head):
            raise ReconcileError("invalid_expected_github_head")
        request = urllib.request.Request(
            f"{self.api_base}/repos/{repository}/pulls/{pr_number}",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "agent-workstream",
            },
        )
        try:
            with self.opener(
                request, timeout=self.timeout, context=default_ssl_context()
            ) as response:
                raw = response.read(MAX_PROVIDER_BYTES + 1)
        except (OSError, urllib.error.URLError) as error:
            raise ReconcileError("github_truth_unavailable") from error
        if len(raw) > MAX_PROVIDER_BYTES:
            raise ReconcileError("github_truth_too_large")
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ReconcileError("github_truth_malformed") from error
        base_repo = ((payload.get("base") or {}).get("repo") or {})
        observed_ids = {str(value) for value in (base_repo.get("id"), base_repo.get("node_id")) if value is not None}
        if provider_repository_id not in observed_ids:
            raise ReconcileError("github_repository_identity_mismatch")
        if str(base_repo.get("full_name", "")).lower() != repository.lower():
            raise ReconcileError("github_repository_coordinate_mismatch")
        if payload.get("number") != pr_number:
            raise ReconcileError("github_pr_mismatch")
        head = str((payload.get("head") or {}).get("sha", ""))
        if head != expected_head:
            raise ReconcileError("github_head_drift")
        merge_sha = payload.get("merge_commit_sha")
        if not payload.get("merged") or not payload.get("merged_at"):
            raise ReconcileError("github_pr_not_merged")
        if not isinstance(merge_sha, str) or not OID.fullmatch(merge_sha):
            raise ReconcileError("github_merge_sha_unavailable")
        return {
            "repository": repository.lower(),
            "provider_repository_id": provider_repository_id,
            "pr_number": pr_number,
            "pr_head": head,
            "merged": True,
            "merge_sha": merge_sha,
        }


def _bounded_command(
    argv: list[str], timeout: float, *, error_prefix: str = "shipyard_truth",
    max_bytes: int = MAX_PROVIDER_BYTES,
) -> bytes:
    if not argv or not all(isinstance(item, str) and item for item in argv):
        raise ReconcileError(f"{error_prefix}_fixed_argv_required")
    if not isinstance(timeout, (int, float)) or timeout <= 0:
        raise ReconcileError(f"{error_prefix}_timeout_invalid")
    environment = {
        key: value for key, value in os.environ.items() if key in SAFE_COMMAND_ENV
    }
    environment["GIT_TERMINAL_PROMPT"] = "0"
    process = subprocess.Popen(
        argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=environment, start_new_session=True,
    )
    assert process.stdout is not None and process.stderr is not None
    selector = selectors.DefaultSelector()
    stdout = bytearray()
    stderr = bytearray()
    for stream, target in ((process.stdout, stdout), (process.stderr, stderr)):
        os.set_blocking(stream.fileno(), False)
        selector.register(stream, selectors.EVENT_READ, target)
    deadline = time.monotonic() + timeout
    failure: str | None = None
    while selector.get_map():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            failure = f"{error_prefix}_timeout"
            break
        events = selector.select(min(0.05, remaining))
        for key, _mask in events:
            try:
                chunk = os.read(key.fileobj.fileno(), 65536)
            except BlockingIOError:
                continue
            if not chunk:
                selector.unregister(key.fileobj)
                key.fileobj.close()
                continue
            key.data.extend(chunk)
            if len(stdout) + len(stderr) > max_bytes:
                failure = f"{error_prefix}_too_large"
                break
        if failure:
            break
        if process.poll() is not None and not events and selector.get_map():
            # A descendant retaining inherited pipes is not part of a bounded
            # one-shot adapter. Terminate the complete session and allow only a
            # short final drain inside the original deadline.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            deadline = min(deadline, time.monotonic() + 0.5)
    if failure:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        for key in list(selector.get_map().values()):
            selector.unregister(key.fileobj)
            key.fileobj.close()
        selector.close()
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=0.5)
            except subprocess.TimeoutExpired as error:
                raise ReconcileError(f"{error_prefix}_process_unreaped") from error
        raise ReconcileError(failure)
    selector.close()
    try:
        process.wait(timeout=max(0.01, deadline - time.monotonic()))
    except subprocess.TimeoutExpired as error:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=0.5)
            except subprocess.TimeoutExpired as reap_error:
                raise ReconcileError(f"{error_prefix}_process_unreaped") from reap_error
        raise ReconcileError(f"{error_prefix}_timeout") from error
    if process.returncode != 0:
        raise ReconcileError(f"{error_prefix}_command_failed")
    return bytes(stdout)


def github_token_from_command(argv: list[str], *, timeout: float = 10.0) -> str:
    """Read one token from a noninteractive fixed-argv helper without logging it."""
    raw = _bounded_command(
        argv, timeout, error_prefix="github_auth", max_bytes=MAX_TOKEN_BYTES,
    )
    try:
        token = raw.decode("utf-8").strip()
    except UnicodeDecodeError as error:
        raise ReconcileError("github_auth_token_malformed") from error
    if not token or any(character.isspace() for character in token):
        raise ReconcileError("github_auth_token_malformed")
    return token


class ShipyardTruthReader:
    def __init__(self, argv: list[str], *, timeout: float = 20.0):
        self.argv = list(argv)
        self.timeout = timeout

    def _payload(self) -> Any:
        try:
            return json.loads(_bounded_command(self.argv, self.timeout))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ReconcileError("shipyard_truth_malformed") from error

    @staticmethod
    def _validate(
        payload: Any, *, repository: str, repository_key_value: str,
        pr_number: int, expected_head: str,
    ) -> dict[str, Any]:
        required = {
            "schema_version", "repository", "repository_key", "pr_number", "head",
            "disposition", "receipt_id", "receipt_sha256",
        }
        if not isinstance(payload, dict) or set(payload) != required or payload.get("schema_version") != 1:
            raise ReconcileError("shipyard_truth_schema_mismatch")
        if str(payload["repository"]).lower() != repository.lower():
            raise ReconcileError("shipyard_repository_mismatch")
        if payload["repository_key"] != repository_key_value:
            raise ReconcileError("shipyard_repository_identity_mismatch")
        if payload["pr_number"] != pr_number or payload["head"] != expected_head:
            raise ReconcileError("shipyard_pr_or_head_mismatch")
        if payload["disposition"] not in {"merged", "already_merged", "landed"}:
            raise ReconcileError("shipyard_not_landed")
        if not isinstance(payload["receipt_id"], str) or not payload["receipt_id"]:
            raise ReconcileError("shipyard_receipt_missing")
        expected_digest = canonical_digest({
            key: value for key, value in payload.items() if key != "receipt_sha256"
        })
        if not DIGEST.fullmatch(str(payload["receipt_sha256"])) or payload["receipt_sha256"] != expected_digest:
            raise ReconcileError("shipyard_receipt_digest_mismatch")
        return payload

    def read(
        self, *, repository: str, repository_key_value: str,
        pr_number: int, expected_head: str,
    ) -> dict[str, Any]:
        return self._validate(
            self._payload(), repository=repository,
            repository_key_value=repository_key_value, pr_number=pr_number,
            expected_head=expected_head,
        )

    def read_many(self, bindings: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Read one versioned aggregate receipt from the fixed-argv adapter."""
        payload = self._payload()
        if (
            not isinstance(payload, dict) or set(payload) != {"schema_version", "receipts"}
            or payload.get("schema_version") != 2
            or not isinstance(payload.get("receipts"), list)
        ):
            raise ReconcileError("shipyard_truth_aggregate_schema_mismatch")
        receipts = payload["receipts"]
        if len(receipts) != len(bindings):
            raise ReconcileError("shipyard_truth_aggregate_keyset_mismatch")
        by_key = {
            item.get("repository_key"): item for item in receipts
            if isinstance(item, dict) and isinstance(item.get("repository_key"), str)
        }
        expected_keys = {f"github.com:id:{item['repository_id']}" for item in bindings}
        if len(by_key) != len(receipts) or set(by_key) != expected_keys:
            raise ReconcileError("shipyard_truth_aggregate_keyset_mismatch")
        return [
            self._validate(
                by_key[f"github.com:id:{binding['repository_id']}"],
                repository=binding["repository"],
                repository_key_value=f"github.com:id:{binding['repository_id']}",
                pr_number=binding["pr"], expected_head=binding["expected_head"],
            )
            for binding in bindings
        ]


def parse_repository_bindings(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Normalize repeatable qualified groups or the legacy single-repo flags."""
    legacy = (args.repository, args.repository_id, args.pr, args.expected_head)
    if args.repository_binding:
        if any(value is not None for value in legacy):
            raise ReconcileError("repository_binding_conflicts_with_single_repository_flags")
        bindings: list[dict[str, Any]] = []
        for raw in args.repository_binding:
            try:
                binding = json.loads(raw)
            except json.JSONDecodeError as error:
                raise ReconcileError("repository_binding_malformed") from error
            if not isinstance(binding, dict) or set(binding) != {
                "repository", "repository_id", "pr", "expected_head",
            }:
                raise ReconcileError("repository_binding_schema_mismatch")
            bindings.append(binding)
    else:
        if any(value is None for value in legacy):
            raise ReconcileError("single_repository_flags_incomplete")
        bindings = [{
            "repository": args.repository, "repository_id": args.repository_id,
            "pr": args.pr, "expected_head": args.expected_head,
        }]
    for binding in bindings:
        if (
            not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", str(binding["repository"]))
            or not isinstance(binding["repository_id"], str) or not binding["repository_id"]
            or not isinstance(binding["pr"], int) or binding["pr"] <= 0
            or not OID.fullmatch(str(binding["expected_head"]))
        ):
            raise ReconcileError("repository_binding_invalid")
    keys = [f"github.com:id:{item['repository_id']}" for item in bindings]
    if len(keys) != len(set(keys)):
        raise ReconcileError("duplicate_repository_binding")
    return bindings


def read_relation_targets(
    client: HttpGraphQLClient, relations: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Resolve every immutable target and reduce its complete peer-edge state."""
    resolved: dict[str, dict[str, Any]] = {}
    for relation in relations:
        if not isinstance(relation, dict) or not isinstance(relation.get("target"), dict):
            raise ReconcileError("invalid_relation_target")
        target = relation["target"]
        key = relation_target_key(target)
        if key in resolved:
            continue
        response = client.execute(
            RELATION_TARGET_QUERY, {"issueId": target.get("issue_id")}
        )
        issue = response.get("issue") if isinstance(response, dict) else None
        team = issue.get("team") if isinstance(issue, dict) else None
        workspace_id = ((team or {}).get("organization") or {}).get("id")
        if (
            not isinstance(issue, dict)
            or issue.get("id") != target.get("issue_id")
            or issue.get("identifier") != target.get("identifier")
            or workspace_id != target.get("workspace_id")
        ):
            raise ReconcileError(f"dangling_relation_target:{target.get('identifier')}")
        team_id = (team or {}).get("id")
        project_id = (issue.get("project") or {}).get("id")
        plan_revision = parse_plan_revision(issue.get("description"))
        if not all(isinstance(item, str) and item for item in (
            team_id, project_id, plan_revision,
        )):
            raise ReconcileError(
                f"relation_target_readback_incomplete:{target.get('identifier')}"
            )
        route = {
            "workspace_id": workspace_id, "team_id": team_id,
            "project_id": project_id, "root_issue_id": issue["id"],
        }
        comments = LinearCommentEventAdapter(
            client, issue_id=issue["identifier"], workspace_id=workspace_id,
            team_id=team_id, project_id=project_id,
        ).comments()
        projection = reduce_projection_comments(
            comments, workstream_id=issue["identifier"],
            expected_plan_revision=plan_revision, authenticated_route=route,
        ).snapshot
        resolved[key] = {
            "workspace_id": workspace_id, "issue_id": issue["id"],
            "identifier": issue["identifier"],
            "relations": projection.get("relations") or [],
        }
    return resolved


def add_relation_target_readback(
    snapshot: dict[str, Any], client: HttpGraphQLClient,
) -> dict[str, Any]:
    value = deepcopy(snapshot)
    relations = value.get("relations") or []
    if relations:
        value["relation_targets"] = read_relation_targets(client, relations)
    return value


def _active_lifecycle(state: Any) -> dict[str, Any] | None:
    current = None
    for event in state.events:
        if event["kind"] == "lifecycle" and event["key"] == "root":
            current = None if event["value"] == TOMBSTONE else event
    return current


def _active_closure_review(state: Any, snapshot_sha256: str) -> dict[str, Any] | None:
    current = None
    for event in state.events:
        if event["kind"] == "closure_review" and event["key"] == snapshot_sha256:
            current = None if event["value"] == TOMBSTONE else event
    return current


def _validate_review_event_order(
    state: Any, *, implementer_session_id: str, review_event: dict[str, Any],
) -> None:
    implementer_positions = [
        index for index, event in enumerate(state.events)
        if event["kind"] == "provenance"
        and event["value"] != TOMBSTONE
        and event["value"].get("session_id") == implementer_session_id
    ]
    review_positions = [
        index for index, event in enumerate(state.events)
        if event["event_id"] == review_event["event_id"]
    ]
    if (
        len(implementer_positions) != 1
        or len(review_positions) != 1
        or implementer_positions[0] >= review_positions[0]
    ):
        raise ReconcileError("independent_review_event_order_invalid")


def _implementer_session(snapshot: dict[str, Any]) -> str:
    checkpoint = snapshot.get("latest_checkpoint")
    if isinstance(checkpoint, dict):
        provenance = checkpoint.get("provenance")
        latest = provenance.get("latest") if isinstance(provenance, dict) else None
        session = latest.get("session_id") if isinstance(latest, dict) else None
        acknowledgement = checkpoint.get("acknowledgement")
        if (
            isinstance(session, str) and session
            and isinstance(acknowledgement, dict)
            and acknowledgement.get("state") == "remote_acknowledged"
        ):
            return session
    provenance = snapshot.get("provenance")
    if isinstance(provenance, list) and len(provenance) == 1:
        session = provenance[0].get("session_id")
        if isinstance(session, str) and session:
            return session
    raise ReconcileError("implementer_session_ambiguous_or_unacknowledged")


def _validate_independent_review(
    receipt: dict[str, Any], *, token: str, snapshot_sha256: str,
    closure_input_sha256: str, repository_heads: dict[str, str],
    repository_truth_sha256: str, implementer_session_id: str,
) -> None:
    single_required = {
        "schema_version", "workstream_id", "snapshot_sha256",
        "closure_input_sha256", "repository_key", "exact_head", "verdict",
        "reviewer_agent", "reviewer_session_id", "implementer_session_id", "reviewed_at",
        "review_artifact_identity", "review_artifact_sha256", "trust_boundary",
        "procedural_independence",
    }
    aggregate_required = {
        "schema_version", "workstream_id", "snapshot_sha256",
        "closure_input_sha256", "repository_heads", "repository_truth_sha256",
        "verdict", "reviewer_agent", "reviewer_session_id",
        "implementer_session_id", "reviewed_at", "review_artifact_identity",
        "review_artifact_sha256", "trust_boundary", "procedural_independence",
    }
    if not isinstance(receipt, dict):
        raise ReconcileError("independent_review_schema_mismatch")
    if len(repository_heads) == 1:
        if set(receipt) != single_required or receipt.get("schema_version") != 1:
            raise ReconcileError("independent_review_schema_mismatch")
    elif set(receipt) != aggregate_required or receipt.get("schema_version") != 2:
        raise ReconcileError("independent_review_schema_mismatch")
    expected = {
        "workstream_id": token, "snapshot_sha256": snapshot_sha256,
        "closure_input_sha256": closure_input_sha256,
        "implementer_session_id": implementer_session_id,
    }
    if len(repository_heads) == 1:
        key, head = next(iter(repository_heads.items()))
        expected.update({"repository_key": key, "exact_head": head})
    else:
        expected.update({
            "repository_heads": repository_heads,
            "repository_truth_sha256": repository_truth_sha256,
        })
    for field, value in expected.items():
        if receipt.get(field) != value:
            raise ReconcileError(f"independent_review_mismatch:{field}")
    if receipt.get("verdict") != "pass":
        raise ReconcileError("independent_review_not_passed")
    if (
        receipt.get("trust_boundary") != "shared_linear_credential"
        or receipt.get("procedural_independence") is not True
        or not DIGEST.fullmatch(str(receipt.get("review_artifact_sha256", "")))
    ):
        raise ReconcileError("independent_review_trust_boundary_invalid")
    for field in (
        "reviewer_agent", "reviewer_session_id", "reviewed_at",
        "review_artifact_identity",
    ):
        if not isinstance(receipt.get(field), str) or not receipt[field]:
            raise ReconcileError(f"independent_review_missing:{field}")
    if receipt["reviewer_session_id"] == implementer_session_id:
        raise ReconcileError("independent_review_same_session")


def _repository_truths(
    scope: dict[str, Any], github: dict[str, Any] | list[dict[str, Any]],
    shipyard: dict[str, Any] | list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Normalize compatibility inputs into one repository-qualified truth set."""
    repositories = scope.get("repositories") or []
    expected = {repository_key(item): item for item in repositories}
    github_items = github if isinstance(github, list) else [github]
    shipyard_items = shipyard if isinstance(shipyard, list) else [shipyard]
    github_by_key: dict[str, dict[str, Any]] = {}
    for item in github_items:
        if not isinstance(item, dict):
            raise ReconcileError("github_truth_schema_mismatch")
        key = f"github.com:id:{item.get('provider_repository_id')}"
        if key in github_by_key:
            raise ReconcileError(f"duplicate_github_truth:{key}")
        github_by_key[key] = item
    shipyard_by_key: dict[str, dict[str, Any]] = {}
    for item in shipyard_items:
        if not isinstance(item, dict) or not isinstance(item.get("repository_key"), str):
            raise ReconcileError("shipyard_truth_schema_mismatch")
        key = item["repository_key"]
        if key in shipyard_by_key:
            raise ReconcileError(f"duplicate_shipyard_truth:{key}")
        shipyard_by_key[key] = item
    if set(github_by_key) != set(expected) or set(shipyard_by_key) != set(expected):
        raise ReconcileError("repository_truth_keyset_mismatch")
    truths: list[dict[str, Any]] = []
    heads: dict[str, str] = {}
    for key, repository in expected.items():
        github_item = github_by_key[key]
        shipyard_item = shipyard_by_key[key]
        expected_head = repository.get("exact_head")
        coordinate = str(repository.get("slug", "")).removeprefix("github.com/").lower()
        if (
            github_item.get("repository") != coordinate
            or github_item.get("provider_repository_id") != repository.get("provider_repository_id")
            or github_item.get("pr_head") != expected_head
            or not github_item.get("merged")
        ):
            raise ReconcileError(f"github_truth_scope_mismatch:{key}")
        if (
            shipyard_item.get("repository", "").lower() != coordinate
            or shipyard_item.get("repository_key") != key
            or shipyard_item.get("pr_number") != github_item.get("pr_number")
            or shipyard_item.get("head") != expected_head
        ):
            raise ReconcileError(f"shipyard_truth_scope_mismatch:{key}")
        heads[key] = expected_head
        truths.append({
            "repository_key": key, "github": deepcopy(github_item),
            "shipyard_receipt": deepcopy(shipyard_item),
        })
    return truths, heads


def reconcile_lifecycle(
    *, snapshot: dict[str, Any], adapter: LinearProjectionAdapter,
    github: dict[str, Any] | list[dict[str, Any]],
    shipyard: dict[str, Any] | list[dict[str, Any]], closure_input: dict[str, Any],
    independent_review: dict[str, Any] | None, created_at: str,
    snapshot_fence: Callable[[], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root = snapshot.get("root") or {}
    token = str(root.get("identifier", "")).upper()
    scope = snapshot.get("scope") or {}
    unresolved_quarantine = snapshot.get("projection_unresolved_quarantine") or []
    if unresolved_quarantine:
        raise ReconcileError("unresolved_v1_projection_quarantine_review_required")
    repositories = scope.get("repositories") or []
    primary_key = scope.get("primary_repository")
    if not token or not any(repository_key(item) == primary_key for item in repositories):
        raise ReconcileError("authoritative_primary_repository_missing")
    repository_truths, repository_heads = _repository_truths(scope, github, shipyard)
    repository_truth_sha256 = canonical_digest(repository_truths)

    snapshot_sha256 = closure_snapshot_digest(snapshot)
    required_closure_input = {"criteria", "evidence", "excluded", "required_child_ids"}
    if not isinstance(closure_input, dict) or set(closure_input) != required_closure_input:
        raise ReconcileError("closure_input_schema_mismatch")
    if (
        not isinstance(closure_input["criteria"], list)
        or not isinstance(closure_input["evidence"], dict)
        or not isinstance(closure_input["excluded"], list)
        or not isinstance(closure_input["required_child_ids"], list)
        or not all(isinstance(item, str) and item for item in closure_input["required_child_ids"])
    ):
        raise ReconcileError("closure_input_schema_mismatch")
    criteria = closure_input["criteria"]
    required_children = closure_input["required_child_ids"]
    if (
        not criteria
        or not all(isinstance(item, str) and item for item in criteria)
        or len(criteria) != len(set(criteria))
        or len(required_children) != len(set(required_children))
        or set(required_children) != set((scope.get("child_ownership") or {}).keys())
    ):
        raise ReconcileError("closure_input_scope_mismatch")
    closure_input_sha256 = canonical_digest(closure_input)
    state = adapter.state()
    if state.revision != snapshot.get("projection_revision"):
        raise ReconcileError("lifecycle_projection_stale_reload_required")
    current = _active_lifecycle(state)
    closure_receipt = None
    status = "Landed — acceptance review required"
    if independent_review is not None:
        implementer_session_id = _implementer_session(snapshot)
        _validate_independent_review(
            independent_review, token=token, snapshot_sha256=snapshot_sha256,
            closure_input_sha256=closure_input_sha256,
            repository_heads=repository_heads,
            repository_truth_sha256=repository_truth_sha256,
            implementer_session_id=implementer_session_id,
        )
        durable_review = _active_closure_review(state, snapshot_sha256)
        if durable_review is None or durable_review["value"] != independent_review:
            raise ReconcileError("independent_review_not_durable")
        _validate_review_event_order(
            state, implementer_session_id=implementer_session_id,
            review_event=durable_review,
        )
        result = closure_review(
            snapshot,
            expected_plan_revision=root.get("plan_revision"),
            criteria=closure_input.get("criteria") or [],
            evidence=closure_input.get("evidence") or {},
            excluded=closure_input.get("excluded") or [],
            semantic_review_invoked=True, semantic_review_passed=True,
            required_child_ids=set(closure_input.get("required_child_ids") or []),
            repository_heads=repository_heads,
        )
        if not result["ok"] or result["receipt"]["final_disposition"] != "Done":
            errors = ",".join(result.get("errors") or ["semantic_closure_refused"])
            raise ReconcileError(f"closure_refused:{errors}")
        closure_receipt = {
            **result["receipt"], "snapshot_sha256": snapshot_sha256,
            "closure_input_sha256": closure_input_sha256,
            "independent_review": independent_review,
        }
        if len(repository_truths) == 1:
            closure_receipt.update({
                "github": repository_truths[0]["github"],
                "shipyard_receipt_sha256": repository_truths[0]["shipyard_receipt"]["receipt_sha256"],
            })
        else:
            closure_receipt.update({
                "repositories": repository_truths,
                "repository_truth_sha256": repository_truth_sha256,
            })
        status = "Done"

    lifecycle = {
        "status": status,
        "closure_input_sha256": closure_input_sha256,
        "snapshot_sha256": snapshot_sha256,
        "independent_review": deepcopy(independent_review),
        "closure_receipt_sha256": (
            canonical_digest(closure_receipt) if closure_receipt is not None else None
        ),
    }
    if len(repository_truths) == 1:
        lifecycle.update({
            "github": repository_truths[0]["github"],
            "shipyard_receipt": repository_truths[0]["shipyard_receipt"],
        })
    else:
        lifecycle.update({
            "repositories": repository_truths,
            "repository_truth_sha256": repository_truth_sha256,
        })
    if snapshot_fence is not None and closure_snapshot_digest(snapshot_fence()) != snapshot_sha256:
        raise ReconcileError("closure_snapshot_changed_reload_required")
    if current is not None and current["value"].get("status") == "Done" and current["value"] != lifecycle:
        if independent_review is None and all(
            current["value"].get(field) == lifecycle.get(field)
            for field in (
                ("github", "shipyard_receipt")
                if len(repository_truths) == 1 else
                ("repositories", "repository_truth_sha256")
            )
        ) and all(
            current["value"].get(field) == lifecycle.get(field)
            for field in ("closure_input_sha256", "snapshot_sha256")
        ):
            return {
                "workstream_id": token, "status": "Done",
                "projection_revision": state.revision, "writes": [],
                "lifecycle": current["value"], "readback_verified": True,
            }
        if not (
            independent_review is not None
            and status == "Done"
            and lifecycle["snapshot_sha256"] != current["value"].get("snapshot_sha256")
        ):
            raise ReconcileError("done_lifecycle_cannot_be_downgraded_or_rewritten")
    if current is not None and current["value"] == lifecycle:
        return {
            "workstream_id": token, "status": status,
            "projection_revision": state.revision, "writes": [],
            "lifecycle": lifecycle, "readback_verified": True,
        }
    event = build_projection_event(
        workstream_id=token, kind="lifecycle", key="root", value=lifecycle,
        plan_revision=adapter.plan_revision, expected_revision=state.revision,
        created_at=created_at,
        supersedes_event_id=current["event_id"] if current else None,
        authority=adapter.authority,
    )
    receipt = adapter.append(event)
    final = adapter.state()
    observed = _active_lifecycle(final)
    if observed is None or observed["event_id"] != event["event_id"] or observed["value"] != lifecycle:
        raise ReconcileError("lifecycle_readback_not_exact")
    if snapshot_fence is not None and closure_snapshot_digest(snapshot_fence()) != snapshot_sha256:
        raise ReconcileError("closure_snapshot_changed_after_append")
    return {
        "workstream_id": token, "status": status,
        "projection_revision": final.revision, "writes": [receipt],
        "lifecycle": lifecycle, "readback_verified": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("token")
    parser.add_argument("--repository", help="GitHub OWNER/REPO (single-repository compatibility)")
    parser.add_argument("--repository-id")
    parser.add_argument("--pr", type=int)
    parser.add_argument("--expected-head")
    parser.add_argument(
        "--repository-binding", action="append", default=[], metavar="JSON",
        help=("repeatable repository-qualified JSON object with repository, "
              "repository_id, pr, and expected_head"),
    )
    github_auth = parser.add_mutually_exclusive_group(required=True)
    github_auth.add_argument("--github-token-command")
    github_auth.add_argument(
        "--github-token-env", choices=("GITHUB_TOKEN", "GH_TOKEN"),
    )
    parser.add_argument("--github-token-arg", action="append", default=[])
    parser.add_argument("--github-token-timeout", type=float, default=10.0)
    parser.add_argument("--plan-source", required=True)
    parser.add_argument("--plan-identity")
    parser.add_argument("--closure-input", required=True)
    parser.add_argument("--review-receipt")
    parser.add_argument("--shipyard-command", required=True)
    parser.add_argument("--shipyard-arg", action="append", default=[])
    parser.add_argument("--shipyard-timeout", type=float, default=20.0)
    parser.add_argument("--config")
    parser.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    args = parser.parse_args()
    try:
        if args.github_token_arg and not args.github_token_command:
            raise ReconcileError("github_token_args_require_command")
        bindings = parse_repository_bindings(args)
        token = extract_token(args.token)
        authenticated_source = plan_payload(args.plan_source, args.plan_identity)["source"]
        api_key = load_linear_api_key()
        if not api_key:
            raise ReconcileError("linear_auth_unavailable")
        github_token = (
            github_token_from_command(
                [args.github_token_command, *args.github_token_arg],
                timeout=args.github_token_timeout,
            )
            if args.github_token_command
            else os.environ.get(args.github_token_env or "", "")
        )
        client = HttpGraphQLClient(api_key, args.linear_endpoint)
        route, _ = resolve_linear_route(config_path=args.config)
        route = resolve_authenticated_issue_route(client, token, route)
        transport = LinearGraphQLTransport(
            client, team_id=route["team_id"], workspace_id=route["workspace_id"],
            project_id=route["project_id"],
        )
        graph = transport.snapshot_for_root(token)
        comments_adapter = LinearCommentEventAdapter(
            client, issue_id=token, workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
        )
        adapter = LinearProjectionAdapter(
            client, issue_id=token, workstream_id=token,
            plan_revision=authenticated_source["sha256"],
            workspace_id=route["workspace_id"], team_id=route["team_id"],
            project_id=route["project_id"], root_issue_id=route["root_issue_id"],
        )
        snapshot = add_material_history(
            graph, comments_adapter.comments(), token,
            authenticated_route=route, authenticated_source=authenticated_source,
            permit_stale_lifecycle_for_reconcile=True,
        )
        snapshot = add_relation_target_readback(snapshot, client)
        if snapshot["root"].get("plan_revision") != authenticated_source["sha256"]:
            raise ReconcileError("root_plan_revision_source_bytes_mismatch")

        def snapshot_fence() -> dict[str, Any]:
            graph_fence, comments_fence = stable_live_readback(
                transport, comments_adapter, token,
            )
            fenced = add_material_history(
                graph_fence, comments_fence, token,
                authenticated_route=route, authenticated_source=authenticated_source,
                permit_stale_lifecycle_for_reconcile=True,
            )
            return add_relation_target_readback(fenced, client)
        github_reader = GitHubTruthReader(github_token)
        github_items = [
            github_reader.read(
                repository=binding["repository"],
                provider_repository_id=binding["repository_id"],
                pr_number=binding["pr"], expected_head=binding["expected_head"],
            )
            for binding in bindings
        ]
        shipyard_reader = ShipyardTruthReader(
            [args.shipyard_command, *args.shipyard_arg], timeout=args.shipyard_timeout,
        )
        shipyard_items = (
            [shipyard_reader.read(
                repository=bindings[0]["repository"],
                repository_key_value=f"github.com:id:{bindings[0]['repository_id']}",
                pr_number=bindings[0]["pr"], expected_head=bindings[0]["expected_head"],
            )]
            if len(bindings) == 1 else shipyard_reader.read_many(bindings)
        )
        closure_input = json.loads(Path(args.closure_input).read_text(encoding="utf-8"))
        independent_review = (
            json.loads(Path(args.review_receipt).read_text(encoding="utf-8"))
            if args.review_receipt else None
        )
        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        activation_receipt = adapter.activate_v2(created_at=created_at)
        if activation_receipt is not None:
            graph_after_activation, comments_after_activation = stable_live_readback(
                transport, comments_adapter, token,
            )
            snapshot = add_material_history(
                graph_after_activation, comments_after_activation, token,
                authenticated_route=route, authenticated_source=authenticated_source,
                permit_stale_lifecycle_for_reconcile=True,
            )
            snapshot = add_relation_target_readback(snapshot, client)
        result = reconcile_lifecycle(
            snapshot=snapshot, adapter=adapter,
            github=github_items[0] if len(github_items) == 1 else github_items,
            shipyard=shipyard_items[0] if len(shipyard_items) == 1 else shipyard_items,
            closure_input=closure_input, independent_review=independent_review,
            created_at=created_at,
            snapshot_fence=snapshot_fence,
        )
        result["cas_activation_write"] = activation_receipt
        json.dump(result, sys.stdout, sort_keys=True, indent=2)
        sys.stdout.write("\n")
        return 0
    except (
        OSError, json.JSONDecodeError, LinearProjectionError, LinearTransportError,
        ResumeError, ReconcileError, ValueError,
    ) as error:
        print(f"workstream reconcile refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
