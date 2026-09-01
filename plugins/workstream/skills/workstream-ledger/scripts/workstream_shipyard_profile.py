#!/usr/bin/env python3
"""Create one private Shipyard LaunchProfileV1 from live workstream authority."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import ctypes
from dataclasses import dataclass
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from typing import Any, Callable
import unicodedata
from urllib.parse import urlparse

from workstream_resume import DEFAULT_RESUME_MAX_BYTES, ResumeError, extract_token
from workstream_scope import ScopeError, canonical_repository
from workstream_child_dependencies import (
    ChildDependencyError, validate_dependency_graph_authority,
)


RESUME_MAX_BYTES = DEFAULT_RESUME_MAX_BYTES
RESUME_MAX_ITEMS = 100
RESUME_OUTPUT_MAX_BYTES = 64 * 1024
HYDRATED_RESUME_MAX_BYTES = 2**31 - 1
HYDRATED_RESUME_OUTPUT_MAX_BYTES = 16 * 1024 * 1024
PROFILE_MAX_BYTES = 64 * 1024
COMMAND_TIMEOUT_SECONDS = 120
SHA1 = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
MODEL = re.compile(r"^[a-z0-9._:-]{1,128}$")
SHIPYARD_HANDLE = re.compile(r"^[A-Z]{1,16}-[1-9][0-9]*$")
SHIPYARD_AGENT_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.:-]{1,256}$")
SHIPYARD_REPOSITORY_COMPONENT = re.compile(
    r"^[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?$",
)
TERMINAL = {
    "done", "completed", "canceled", "cancelled", "duplicate", "superseded",
}
EFFORTS = {"low", "medium", "high", "xhigh", "max", "ultra"}
AMBIGUOUS = {"unknown", "unavailable", "unset", "none", "null", "n/a"}


class ShipyardProfileError(ValueError):
    """The resume or local checkout cannot authorize a launch profile."""


def _is_control(character: str) -> bool:
    return unicodedata.category(character) == "Cc"


@dataclass(frozen=True)
class GitIdentity:
    root: Path
    repository_coordinate: str
    repository: str
    head: str
    branch: str


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def _authority_digest(domain: str, value: Any) -> str:
    return hashlib.sha256(domain.encode("ascii") + b"\0" + _canonical(value)).hexdigest()


def _resume_context_digest(context: dict[str, Any]) -> str:
    """Digest resume authority with set-like child issues in stable ID order."""
    _require_hydrated_resume(context)
    children = context.get("children")
    if not isinstance(children, list):
        raise ShipyardProfileError("resume_children_missing")
    keyed_children: list[tuple[str, dict[str, Any]]] = []
    identifiers: set[str] = set()
    for child in children:
        if not isinstance(child, dict):
            raise ShipyardProfileError("resume_child_is_not_an_object")
        identifier = _metadata(child.get("identifier"), "resume_child_identifier").upper()
        if identifier in identifiers:
            raise ShipyardProfileError("duplicate_resume_child_identifier")
        identifiers.add(identifier)
        keyed_children.append((identifier, child))
    normalized = dict(context)
    normalized["children"] = [
        child for _, child in sorted(keyed_children, key=lambda item: item[0])
    ]
    return _authority_digest("agent-workstream-resume-context-v1", normalized)


def _require_hydrated_resume(context: dict[str, Any]) -> None:
    envelope = (context.get("context_schema") or {}).get("envelope")
    deferred = context.get("deferred_audit_detail")
    deferred_state = deferred.get("state") if isinstance(deferred, dict) else None
    if envelope is None and deferred_state in {None, "none"}:
        return
    route = deferred.get("audit_route") if isinstance(deferred, dict) else None
    if (
        (envelope is not None and envelope not in {
            "verbose_current_detail_v1", "bounded_authority_v1",
            "fixed_frontier_authority_v1",
        })
        or deferred_state not in {
            "verbose_current_detail_deferred",
            "bounded_authority_envelope",
            "fixed_frontier_authority_envelope",
        }
        or not isinstance(route, dict)
        or route.get("representation") != "compact_validated"
        or route.get("launcher") != "current_workstream_resume_skill_script"
        or not isinstance(route.get("args"), list)
        or not route["args"]
        or not all(isinstance(part, str) and part for part in route["args"])
    ):
        raise ShipyardProfileError("resume_envelope_hydration_route_missing")
    # Profile construction binds exact checkpoint/worktree/scope values. It
    # must never reinterpret digest-bound excerpts as those exact values.
    raise ShipyardProfileError("resume_envelope_requires_compact_validated_hydration")


def _positive_int(value: Any, label: str, *, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > 2**64 - 1
    ):
        raise ShipyardProfileError(f"invalid_{label}")
    return value


def _metadata(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > 256
        or "\0" in value
        or any(_is_control(character) for character in value)
    ):
        raise ShipyardProfileError(f"invalid_{label}")
    return value


def _concrete_metadata(value: Any, label: str) -> str:
    result = _metadata(value, label)
    if result.strip().lower() in AMBIGUOUS:
        raise ShipyardProfileError(f"ambiguous_{label}")
    return result


def _concrete_text(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value.encode("utf-8")) > 4096
        or "\0" in value
        or any(_is_control(character) for character in value)
        or value.strip().lower() in AMBIGUOUS
    ):
        raise ShipyardProfileError(f"invalid_{label}")
    return value


def _canonical_context_url(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096:
        raise ShipyardProfileError("invalid_context_url")
    parsed = urlparse(value)
    if (
        not value.startswith("https://")
        or parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "?" in value
        or "#" in value
        or any(character.isspace() or _is_control(character) for character in value)
    ):
        raise ShipyardProfileError("invalid_context_url")
    return value


def _git_value(path: Path, arguments: list[str]) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(path), *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ShipyardProfileError("git_inspection_unavailable") from error
    if result.returncode != 0:
        raise ShipyardProfileError(f"git_authority_unavailable:{arguments[0]}")
    return result.stdout.strip()


def inspect_git_worktree(
    path: str | Path,
    *,
    git_value: Callable[[Path, list[str]], str] = _git_value,
) -> GitIdentity:
    try:
        requested = Path(path).expanduser().resolve(strict=True)
        root = Path(git_value(requested, ["rev-parse", "--show-toplevel"])).resolve(
            strict=True
        )
    except OSError as error:
        raise ShipyardProfileError("worktree_path_unavailable") from error
    if not root.is_dir():
        raise ShipyardProfileError("worktree_root_unavailable")
    if (
        len(str(root).encode("utf-8")) > 4096
        or "\0" in str(root)
        or any(_is_control(character) for character in str(root))
    ):
        raise ShipyardProfileError("worktree_path_is_not_bounded")
    if git_value(root, ["status", "--porcelain=v1", "--untracked-files=normal"]):
        raise ShipyardProfileError("worktree_not_clean")

    head = git_value(root, ["rev-parse", "HEAD"]).lower()
    if not SHA1.fullmatch(head):
        raise ShipyardProfileError("worktree_head_is_not_sha1")
    branch = _metadata(
        git_value(root, ["symbolic-ref", "--quiet", "--short", "HEAD"]),
        "worktree_lineage_id",
    )
    try:
        coordinate = canonical_repository(git_value(root, ["remote", "get-url", "origin"]))
    except ScopeError as error:
        raise ShipyardProfileError("worktree_origin_is_not_canonical") from error
    host, owner, repository_name = coordinate.split("/", 2)
    if host != "github.com":
        raise ShipyardProfileError("shipyard_requires_github_origin")
    if not all(
        len(component.encode("utf-8")) <= 100
        and SHIPYARD_REPOSITORY_COMPONENT.fullmatch(component)
        for component in (owner, repository_name)
    ):
        raise ShipyardProfileError("worktree_repository_is_not_shipyard_compatible")

    lineage_key = f"branch.{branch}.pulpWorktree"
    status_value = git_value(root, ["config", "--local", "--get", f"{lineage_key}Status"])
    durable_head = git_value(
        root, ["config", "--local", "--get", f"{lineage_key}DurableSha"],
    ).lower()
    recorded_path = git_value(
        root, ["config", "--local", "--get", f"{lineage_key}LastPath"],
    )
    try:
        canonical_recorded_path = Path(recorded_path).expanduser().resolve(strict=True)
    except OSError as error:
        raise ShipyardProfileError("worktree_lineage_path_unavailable") from error
    if status_value != "active" or durable_head != head or canonical_recorded_path != root:
        raise ShipyardProfileError("worktree_lineage_not_active_at_exact_head")

    return GitIdentity(
        root=root,
        repository_coordinate=coordinate,
        repository=f"{owner}/{repository_name}",
        head=head,
        branch=branch,
    )


def _resume_command(
    token: str,
    *,
    config: str | None,
    linear_endpoint: str,
    plan_source: str | None,
    plan_identity: str | None,
    max_bytes: int = RESUME_MAX_BYTES,
    max_items: int = RESUME_MAX_ITEMS,
) -> list[str]:
    command = [
        sys.executable,
        str(Path(__file__).with_name("workstream_resume.py")),
        token,
        "--max-bytes",
        str(max_bytes),
        "--max-items",
        str(max_items),
        "--linear-endpoint",
        linear_endpoint,
    ]
    for flag, value in (
        ("--config", config),
        ("--plan-source", plan_source),
        ("--plan-identity", plan_identity),
    ):
        if value is not None:
            command.extend([flag, value])
    return command


def load_authenticated_resume(
    token: str,
    *,
    repo_path: str | Path,
    config: str | None = None,
    linear_endpoint: str = "https://api.linear.app/graphql",
    plan_source: str | None = None,
    plan_identity: str | None = None,
) -> dict[str, Any]:
    """Run the existing full-authority resume path without an inspection fallback."""
    def run(command: list[str], output_limit: int) -> dict[str, Any]:
        try:
            result = subprocess.run(
                command, cwd=Path(repo_path).expanduser(), check=False,
                capture_output=True, timeout=COMMAND_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise ShipyardProfileError("authenticated_resume_unavailable") from error
        if result.returncode != 0:
            detail = result.stderr.decode(
                "utf-8", "replace",
            ).strip().replace("\n", " ")[:2048]
            raise ShipyardProfileError(
                f"authenticated_resume_refused:{detail or result.returncode}"
            )
        if len(result.stdout) > output_limit:
            raise ShipyardProfileError("authenticated_resume_over_budget")
        try:
            observed = json.loads(result.stdout)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ShipyardProfileError("authenticated_resume_malformed") from error
        if not isinstance(observed, dict):
            raise ShipyardProfileError("authenticated_resume_malformed")
        return observed

    command_kwargs = {
        "config": config, "linear_endpoint": linear_endpoint,
        "plan_source": plan_source, "plan_identity": plan_identity,
    }
    context = run(
        _resume_command(token, **command_kwargs), RESUME_OUTPUT_MAX_BYTES,
    )
    envelope = (context.get("context_schema") or {}).get("envelope")
    deferred = context.get("deferred_audit_detail")
    deferred_state = deferred.get("state") if isinstance(deferred, dict) else None
    if envelope is not None or deferred_state not in {None, "none"}:
        expected_digest = (
            deferred.get("full_context_sha256")
            if isinstance(deferred, dict) else None
        )
        if not isinstance(expected_digest, str) or not SHA256.fullmatch(expected_digest):
            raise ShipyardProfileError("resume_envelope_hydration_digest_missing")
        hydrated = run(_resume_command(
            token, **command_kwargs,
            max_bytes=HYDRATED_RESUME_MAX_BYTES,
            max_items=RESUME_MAX_ITEMS,
        ), HYDRATED_RESUME_OUTPUT_MAX_BYTES)
        if (hydrated.get("context_schema") or {}).get("envelope") is not None:
            raise ShipyardProfileError("resume_envelope_hydration_malformed")
        observed_digest = hashlib.sha256(_canonical(hydrated)).hexdigest()
        if observed_digest != expected_digest or any(
            hydrated.get(field) != context.get(field)
            for field in ("workstream_id", "plan_revision", "root_revision")
        ):
            raise ShipyardProfileError("resume_envelope_hydration_mismatch")
        context = hydrated
    return context


def _validate_scope(context: dict[str, Any], git: GitIdentity) -> None:
    scope = context.get("scope")
    repositories = scope.get("repositories") if isinstance(scope, dict) else None
    if not isinstance(repositories, list) or not repositories:
        raise ShipyardProfileError("resume_repository_scope_missing")
    exact = [
        item for item in repositories
        if isinstance(item, dict) and item.get("slug") == git.repository_coordinate
    ]
    if len(exact) != 1:
        alias_matches = [
            item for item in repositories
            if isinstance(item, dict)
            and git.repository_coordinate in (item.get("aliases") or [])
        ]
        if alias_matches:
            raise ShipyardProfileError("worktree_origin_is_noncanonical_alias")
        raise ShipyardProfileError("worktree_repository_not_in_resume_scope")
    if exact[0].get("exact_head") != git.head:
        raise ShipyardProfileError("resume_scope_head_mismatch")


def _validate_dependency_graph(
    context: dict[str, Any], route: dict[str, Any], plan_revision: str,
    material_revision: int,
) -> tuple[int, str, str]:
    graph = context.get("dependency_graph")
    if not isinstance(graph, dict):
        raise ShipyardProfileError("resume_dependency_graph_missing")
    revision = graph.get("revision")
    graph_sha256 = graph.get("sha256")
    dependency_authority = context.get("dependency_authority")
    if graph.get("relations") or graph.get("authorization_batches"):
        if (
            not isinstance(dependency_authority, dict)
            or set(dependency_authority) != {
                "owned_children", "authorization_events", "material_event_ids",
            }
        ):
            raise ShipyardProfileError("resume_dependency_graph_invalid")
    else:
        dependency_authority = dependency_authority or {
            "owned_children": [], "authorization_events": [],
            "material_event_ids": [],
        }
    try:
        validate_dependency_graph_authority(
            graph,
            authority={**route, "root_identifier": context.get("workstream_id")},
            plan_revision=plan_revision,
            expected_projection_events=dependency_authority[
                "authorization_events"
            ],
            expected_material_event_ids=dependency_authority[
                "material_event_ids"
            ],
            expected_owned_identifiers=set(
                (context.get("scope") or {}).get("child_ownership", {})
            ),
            expected_owned_children=dependency_authority["owned_children"],
            expected_frontier={
                "material_revision": material_revision,
                "projection_revision": context.get("projection_revision"),
                "graph_revision": revision,
                "graph_sha256": graph_sha256,
            },
        )
    except (ChildDependencyError, KeyError, TypeError) as error:
        raise ShipyardProfileError("resume_dependency_graph_invalid")
    return revision, graph_sha256, _authority_digest(
        "agent-workstream-dependency-graph-v1", graph,
    )


def _validate_current_resume(
    context: dict[str, Any], token: str, git: GitIdentity,
) -> tuple[dict[str, Any], str, str, int, str]:
    _require_hydrated_resume(context)
    if context.get("resume_authority") != "full":
        raise ShipyardProfileError("resume_is_not_full_authority")
    if context.get("workstream_id") != token:
        raise ShipyardProfileError("resume_workstream_mismatch")
    plan_revision = context.get("plan_revision")
    if not isinstance(plan_revision, str) or not SHA256.fullmatch(plan_revision):
        raise ShipyardProfileError("invalid_plan_revision")
    root_revision = _positive_int(
        context.get("root_revision"), "root_revision", allow_zero=True,
    )
    _positive_int(context.get("issue_revision"), "issue_revision", allow_zero=True)
    material_revision = _positive_int(
        context.get("material_event_revision"),
        "material_event_revision",
        allow_zero=True,
    )
    _positive_int(context.get("projection_revision"), "projection_revision")
    if root_revision != material_revision:
        raise ShipyardProfileError("resume_material_revision_mismatch")
    status_value = _concrete_metadata(context.get("status"), "resume_status")
    if status_value.strip().lower() in TERMINAL:
        raise ShipyardProfileError("workstream_is_terminal")
    next_action = _concrete_text(context.get("next_action"), "resume_next_action")
    _canonical_context_url(context.get("context_url"))

    source = context.get("source")
    authenticated_source = context.get("authenticated_source")
    if (
        not isinstance(source, dict)
        or source.get("sha256") != plan_revision
        or not isinstance(authenticated_source, dict)
        or authenticated_source.get("sha256") != plan_revision
    ):
        raise ShipyardProfileError("resume_plan_source_not_authenticated")
    route = context.get("authenticated_route")
    if not isinstance(route, dict) or not all(
        isinstance(route.get(field), str) and route[field]
        for field in ("workspace_id", "team_id", "project_id", "root_issue_id")
    ):
        raise ShipyardProfileError("resume_route_not_authenticated")
    availability = context.get("surface_availability")
    required_surfaces = {
        "scope", "relations", "choice_events", "evidence_contracts",
        "material_events", "dependency_graph", "latest_checkpoint",
    }
    if (
        not isinstance(availability, dict)
        or not required_surfaces.issubset(availability)
        or any(value != "available" for value in availability.values())
    ):
        raise ShipyardProfileError("resume_surface_incomplete")
    projection_recovery = context.get("projection_recovery")
    if not isinstance(projection_recovery, dict) or projection_recovery.get("state") != "current":
        raise ShipyardProfileError("resume_projection_not_current")
    if context.get("lifecycle_recovery") is not None:
        raise ShipyardProfileError("resume_lifecycle_not_current")
    quarantine = context.get("projection_quarantine")
    if not isinstance(quarantine, dict) or quarantine.get("count") != 0:
        raise ShipyardProfileError("resume_projection_quarantined")
    obligations = context.get("uncheckpointed_material_obligations")
    if not isinstance(obligations, list) or obligations:
        raise ShipyardProfileError("resume_has_uncheckpointed_obligations")
    _validate_dependency_graph(context, route, plan_revision, material_revision)

    recovery = context.get("checkpoint_recovery")
    checkpoint = context.get("latest_checkpoint")
    if (
        not isinstance(recovery, dict)
        or recovery.get("state") != "current"
        or not isinstance(checkpoint, dict)
    ):
        raise ShipyardProfileError("current_checkpoint_missing")
    if checkpoint.get("workstream_id") != token or checkpoint.get("plan_revision") != plan_revision:
        raise ShipyardProfileError("checkpoint_authority_mismatch")
    checkpoint_status = checkpoint.get("status")
    checkpoint_next_action = _concrete_text(
        checkpoint.get("next_action"), "checkpoint_next_action",
    )
    if (
        not isinstance(checkpoint_status, dict)
        or checkpoint_status.get("after") != status_value
        or checkpoint_next_action != next_action
    ):
        raise ShipyardProfileError("checkpoint_current_view_mismatch")
    checkpoint_revision = _positive_int(
        checkpoint.get("root_revision"), "checkpoint_root_revision", allow_zero=True,
    )
    if checkpoint_revision != root_revision:
        raise ShipyardProfileError("checkpoint_does_not_cover_current_revision")
    checkpoint_id = _metadata(checkpoint.get("checkpoint_event_id"), "checkpoint_id")
    acknowledgement = checkpoint.get("acknowledgement")
    applied_revision = (
        acknowledgement.get("applied_revision")
        if isinstance(acknowledgement, dict) else None
    )
    if (
        not isinstance(acknowledgement, dict)
        or acknowledgement.get("state") != "remote_acknowledged"
        or not isinstance(acknowledgement.get("remote_id"), str)
        or not acknowledgement["remote_id"]
    ):
        raise ShipyardProfileError("checkpoint_not_remote_acknowledged")
    applied_revision = _positive_int(
        applied_revision, "checkpoint_applied_revision", allow_zero=True,
    )
    if applied_revision < root_revision:
        raise ShipyardProfileError("checkpoint_not_remote_acknowledged")

    worktree = checkpoint.get("worktree")
    if not isinstance(worktree, dict) or worktree.get("state") != "safe":
        raise ShipyardProfileError("checkpoint_worktree_not_safe")
    try:
        checkpoint_path = Path(str(worktree.get("path", ""))).expanduser().resolve(strict=True)
    except OSError as error:
        raise ShipyardProfileError("checkpoint_worktree_unavailable") from error
    if (
        checkpoint_path != git.root
        or worktree.get("branch") != git.branch
        or worktree.get("head") != git.head
        or checkpoint.get("exact_head") != git.head
    ):
        raise ShipyardProfileError("checkpoint_worktree_mismatch")

    provenance = checkpoint.get("provenance")
    latest = provenance.get("latest") if isinstance(provenance, dict) else None
    generation = provenance.get("count") if isinstance(provenance, dict) else None
    generation = _positive_int(generation, "checkpoint_generation")
    if (
        not isinstance(latest, dict)
        or latest.get("event_id") != checkpoint_id
        or latest.get("worktree") != worktree
    ):
        raise ShipyardProfileError("checkpoint_provenance_mismatch")
    provider = _concrete_metadata(latest.get("agent"), "agent_provider")
    if provider not in {"codex", "claude"}:
        raise ShipyardProfileError("unsupported_agent_provider")
    session_id = _concrete_metadata(latest.get("session_id"), "provider_session_id")
    if not SHIPYARD_AGENT_IDENTIFIER.fullmatch(session_id):
        raise ShipyardProfileError("provider_session_id_is_not_shipyard_compatible")
    _concrete_metadata(latest.get("provider"), "checkpoint_provider")

    disposition = context.get("disposition")
    recovered_checkpoint = (
        disposition.get("recovered_from_checkpoint")
        if isinstance(disposition, dict) else None
    )
    if (
        not isinstance(disposition, dict)
        or disposition.get("disposition") != "attach"
        or disposition.get("remote_head") != git.head
        or recovered_checkpoint != checkpoint_id
    ):
        raise ShipyardProfileError("resume_disposition_mismatch")
    _validate_scope(context, git)
    checkpoint_digest = _authority_digest(
        "agent-workstream-checkpoint-v1", checkpoint,
    )
    return checkpoint, provider, session_id, generation, checkpoint_digest


def build_launch_profile(
    context: dict[str, Any],
    token: str,
    git: GitIdentity,
    *,
    model: str,
    reasoning_effort: str,
) -> dict[str, Any]:
    normalized = extract_token(token)
    if (
        len(normalized.encode("utf-8")) > 124
        or not SHIPYARD_HANDLE.fullmatch(normalized)
    ):
        raise ShipyardProfileError("workstream_handle_is_not_shipyard_compatible")
    if (
        git.repository_coordinate != f"github.com/{git.repository}"
        or len(str(git.root).encode("utf-8")) > 4096
        or any(_is_control(character) for character in str(git.root))
    ):
        raise ShipyardProfileError("git_identity_is_not_shipyard_compatible")
    repository_parts = git.repository.split("/")
    if len(repository_parts) != 2 or not all(
        len(component.encode("utf-8")) <= 100
        and SHIPYARD_REPOSITORY_COMPONENT.fullmatch(component)
        for component in repository_parts
    ):
        raise ShipyardProfileError("git_identity_is_not_shipyard_compatible")
    if not isinstance(model, str) or not MODEL.fullmatch(model):
        raise ShipyardProfileError("invalid_model")
    if not isinstance(reasoning_effort, str) or reasoning_effort not in EFFORTS:
        raise ShipyardProfileError("invalid_reasoning_effort")
    checkpoint, provider, session_id, generation, checkpoint_digest = (
        _validate_current_resume(context, normalized, git)
    )
    dependency_graph_revision, dependency_graph_sha256, dependency_graph_digest = (
        _validate_dependency_graph(
            context, context["authenticated_route"], context["plan_revision"],
            context["material_event_revision"],
        )
    )
    if provider == "claude" and reasoning_effort == "ultra":
        raise ShipyardProfileError("claude_ultra_effort_unsupported")

    if provider == "codex":
        launch_argv = [
            "codex", "--model", model, "-c",
            f'model_reasoning_effort="{reasoning_effort}"',
        ]
        resume_argv = [
            "codex", "resume", "--model", model, "-c",
            f'model_reasoning_effort="{reasoning_effort}"', session_id,
        ]
    else:
        launch_argv = ["claude", "--model", model, "--effort", reasoning_effort]
        resume_argv = [
            "claude", "--model", model, "--effort", reasoning_effort,
            "--resume", session_id,
        ]

    resume_digest = _resume_context_digest(context)
    continuation_authority = {
        "schema_version": 1,
        "workstream_handle": normalized,
        "checkpoint_id": checkpoint["checkpoint_event_id"],
        "checkpoint_generation": generation,
        "checkpoint_digest": checkpoint_digest,
        "resume_context_digest": resume_digest,
        "dependency_graph_digest": dependency_graph_digest,
        "repository": git.repository,
        "head_sha": git.head,
    }
    success_digest = _authority_digest(
        "agent-workstream-continuation-v1",
        {**continuation_authority, "outcome": "success"},
    )
    failure_digest = _authority_digest(
        "agent-workstream-continuation-v1",
        {**continuation_authority, "outcome": "failure"},
    )
    profile = {
        "schema_version": 1,
        "launch_argv": launch_argv,
        "resume_argv": resume_argv,
        "provider": {
            "provider_id": provider,
            "model_id": model,
            "reasoning_effort": reasoning_effort,
        },
        "session": {
            "agent_provider": provider,
            "provider_session_id": session_id,
        },
        "checkpoint": {
            "checkpoint_id": checkpoint["checkpoint_event_id"],
            "generation": generation,
            "digest": checkpoint_digest,
        },
        "worktree": {
            "repository": git.repository,
            "path": str(git.root),
            "head_sha": git.head,
            "lineage_id": git.branch,
        },
        "continuation_bootstrap": {
            "workstream_handle": normalized,
            "context_url": context["context_url"],
            "plan_sha256": context["plan_revision"],
            "root_revision": context["root_revision"],
            "issue_revision": context["issue_revision"],
            "projection_revision": context["projection_revision"],
            "material_event_revision": context["material_event_revision"],
            "dependency_graph_revision": dependency_graph_revision,
            "dependency_graph_sha256": dependency_graph_sha256,
            "dependency_graph_digest": dependency_graph_digest,
            "checkpoint_id": checkpoint["checkpoint_event_id"],
            "checkpoint_generation": generation,
            "checkpoint_digest": checkpoint_digest,
            "repository": git.repository,
            "head_sha": git.head,
            "expected_resume_context_digest": resume_digest,
            "success_continuation_digest": success_digest,
            "failure_continuation_digest": failure_digest,
        },
        "recovery_policy": "exact_session_then_fresh_checkpoint",
    }
    if len(_canonical(profile)) > PROFILE_MAX_BYTES:
        raise ShipyardProfileError("launch_profile_over_budget")
    return profile


def _assert_no_darwin_extended_acl(descriptor: int, label: str) -> None:
    """Reject any macOS extended ACL; mode bits alone do not bound its grants."""
    if sys.platform != "darwin":
        return
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        acl_get_fd_np = libc.acl_get_fd_np
        acl_get_fd_np.argtypes = [ctypes.c_int, ctypes.c_int]
        acl_get_fd_np.restype = ctypes.c_void_p
        acl_free = libc.acl_free
        acl_free.argtypes = [ctypes.c_void_p]
        acl_free.restype = ctypes.c_int
    except (AttributeError, OSError) as error:
        raise ShipyardProfileError("owner_only_acl_check_unavailable") from error

    ctypes.set_errno(0)
    acl = acl_get_fd_np(descriptor, 0x00000100)  # ACL_TYPE_EXTENDED
    if acl:
        acl_free(acl)
        raise ShipyardProfileError(f"{label}_has_extended_acl")
    if ctypes.get_errno() != errno.ENOENT:
        raise ShipyardProfileError(f"{label}_acl_state_unavailable")


def _private_parent(path: Path) -> tuple[Path, int, tuple[int, int]]:
    if sys.platform != "darwin" or not hasattr(os, "geteuid"):
        raise ShipyardProfileError("owner_only_output_unsupported_on_this_platform")
    descriptor = -1
    try:
        parent = path.parent.resolve(strict=True)
        descriptor = os.open(
            parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        metadata = os.fstat(descriptor)
        path_metadata = os.lstat(parent)
    except OSError as error:
        if descriptor >= 0:
            os.close(descriptor)
        raise ShipyardProfileError("output_parent_unavailable") from error
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or (metadata.st_dev, metadata.st_ino)
        != (path_metadata.st_dev, path_metadata.st_ino)
    ):
        os.close(descriptor)
        raise ShipyardProfileError("output_parent_must_be_private_and_owner_only")
    try:
        _assert_no_darwin_extended_acl(descriptor, "output_parent")
    except ShipyardProfileError:
        os.close(descriptor)
        raise
    return parent, descriptor, (metadata.st_dev, metadata.st_ino)


def _revalidate_private_parent(
    parent: Path, descriptor: int, identity: tuple[int, int],
) -> None:
    try:
        metadata = os.fstat(descriptor)
        path_metadata = os.lstat(parent)
    except OSError as error:
        raise ShipyardProfileError("output_parent_changed_during_publication") from error
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or (metadata.st_dev, metadata.st_ino) != identity
        or (path_metadata.st_dev, path_metadata.st_ino) != identity
    ):
        raise ShipyardProfileError("output_parent_changed_during_publication")
    _assert_no_darwin_extended_acl(descriptor, "output_parent")


def write_private_profile(path: str | Path, profile: dict[str, Any]) -> Path:
    requested = Path(path).expanduser()
    if not requested.name or requested.name in {".", ".."}:
        raise ShipyardProfileError("invalid_output_path")
    parent, directory_descriptor, parent_identity = _private_parent(requested)
    destination = parent / requested.name
    try:
        os.lstat(destination.name, dir_fd=directory_descriptor)
    except FileNotFoundError:
        pass
    except OSError as error:
        os.close(directory_descriptor)
        raise ShipyardProfileError("output_path_unavailable") from error
    else:
        os.close(directory_descriptor)
        raise ShipyardProfileError("output_path_already_exists")

    payload = json.dumps(
        profile, ensure_ascii=False, sort_keys=True, indent=2,
    ).encode("utf-8") + b"\n"
    if len(payload) > PROFILE_MAX_BYTES:
        raise ShipyardProfileError("launch_profile_over_budget")
    descriptor = -1
    temporary: str | None = None
    temporary_name: str | None = None
    published = False
    succeeded = False
    try:
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{destination.name}.", suffix=".tmp", dir=parent,
        )
        temporary_name = Path(temporary).name
        os.fchmod(descriptor, 0o600)
        temporary_metadata = os.fstat(descriptor)
        named_temporary_metadata = os.stat(
            temporary_name, dir_fd=directory_descriptor, follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(temporary_metadata.st_mode)
            or temporary_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(temporary_metadata.st_mode) & 0o077
            or temporary_metadata.st_nlink != 1
            or (temporary_metadata.st_dev, temporary_metadata.st_ino)
            != (named_temporary_metadata.st_dev, named_temporary_metadata.st_ino)
        ):
            raise ShipyardProfileError("temporary_output_is_not_private_and_canonical")
        _assert_no_darwin_extended_acl(descriptor, "temporary_output")
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        _revalidate_private_parent(parent, directory_descriptor, parent_identity)
        # Publish by same-directory hard link so a concurrent same-UID writer
        # cannot turn the preceding nonexistence check into an overwrite.
        os.link(
            temporary_name,
            destination.name,
            src_dir_fd=directory_descriptor,
            dst_dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
        published = True
        os.unlink(temporary_name, dir_fd=directory_descriptor)
        temporary = None
        temporary_name = None
        os.fsync(directory_descriptor)
        _revalidate_private_parent(parent, directory_descriptor, parent_identity)
        output_descriptor = os.open(
            destination.name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_descriptor,
        )
        try:
            metadata = os.fstat(output_descriptor)
            _assert_no_darwin_extended_acl(output_descriptor, "output_file")
        finally:
            os.close(output_descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) & 0o077
            or metadata.st_nlink != 1
            or (metadata.st_dev, metadata.st_ino)
            != (temporary_metadata.st_dev, temporary_metadata.st_ino)
        ):
            raise ShipyardProfileError("output_file_is_not_private_and_canonical")
        succeeded = True
    except (ShipyardProfileError, OSError) as error:
        if published and not succeeded:
            try:
                current = os.stat(
                    destination.name,
                    dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
                if (current.st_dev, current.st_ino) == (
                    temporary_metadata.st_dev, temporary_metadata.st_ino,
                ):
                    os.unlink(destination.name, dir_fd=directory_descriptor)
                    os.fsync(directory_descriptor)
            except FileNotFoundError:
                pass
            except OSError as cleanup_error:
                raise ShipyardProfileError("atomic_output_cleanup_failed") from cleanup_error
        if isinstance(error, ShipyardProfileError):
            raise
        raise ShipyardProfileError("atomic_output_failed") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary_name is not None:
            try:
                os.unlink(temporary_name, dir_fd=directory_descriptor)
            except FileNotFoundError:
                pass
        os.close(directory_descriptor)
    return destination


def _absolute_local_option(value: str | None) -> str | None:
    if value is None or "://" in value:
        return value
    return str(Path(value).expanduser().resolve())


def _output_outside_worktree(path: str | Path, worktree: Path) -> Path:
    requested = Path(path).expanduser()
    if not requested.name or requested.name in {".", ".."}:
        raise ShipyardProfileError("invalid_output_path")
    try:
        destination = requested.parent.resolve(strict=True) / requested.name
    except OSError as error:
        raise ShipyardProfileError("output_parent_unavailable") from error
    if destination == worktree or worktree in destination.parents:
        raise ShipyardProfileError("output_must_be_outside_bound_worktree")
    return destination


def _validate_ambient_session(
    profile: dict[str, Any], environment: Mapping[str, str],
) -> None:
    codex_value = environment.get("CODEX_THREAD_ID", "")
    claude_value = environment.get("CLAUDE_CODE_SESSION_ID", "")
    codex = codex_value if codex_value.strip() else ""
    claude = claude_value if claude_value.strip() else ""
    if codex and claude:
        raise ShipyardProfileError("ambient_agent_session_is_ambiguous")
    if not codex and not claude:
        return
    provider = "codex" if codex else "claude"
    session_id = codex or claude
    session = profile["session"]
    if (
        session["agent_provider"] != provider
        or session["provider_session_id"] != session_id
    ):
        raise ShipyardProfileError("checkpoint_does_not_match_ambient_agent_session")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("handle", help="one workstream token, URL, or token-bearing title")
    parser.add_argument("--repo-path", default=".", help="participating Git worktree")
    parser.add_argument("--model", required=True, help="canonical provider model token")
    parser.add_argument(
        "--reasoning-effort", required=True, choices=sorted(EFFORTS),
    )
    parser.add_argument("--output", required=True, help="new file in an owner-only directory")
    parser.add_argument("--config", help="workstream config path")
    parser.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    parser.add_argument("--plan-source", help="authenticated plan path or immutable URL")
    parser.add_argument("--plan-identity", help="immutable identity for --plan-source")
    args = parser.parse_args(argv)
    try:
        token = extract_token(args.handle)
        repo_path = Path(args.repo_path).expanduser().resolve(strict=True)
        context = load_authenticated_resume(
            token,
            repo_path=repo_path,
            config=_absolute_local_option(args.config),
            linear_endpoint=args.linear_endpoint,
            plan_source=_absolute_local_option(args.plan_source),
            plan_identity=args.plan_identity,
        )
        git = inspect_git_worktree(repo_path)
        output_path = _output_outside_worktree(args.output, git.root)
        profile = build_launch_profile(
            context, token, git, model=args.model,
            reasoning_effort=args.reasoning_effort,
        )
        _validate_ambient_session(profile, os.environ)
        if inspect_git_worktree(repo_path) != git:
            raise ShipyardProfileError("worktree_changed_during_profile_generation")
        output = write_private_profile(output_path, profile)
    except (
        OSError,
        ResumeError,
        ScopeError,
        ShipyardProfileError,
        ValueError,
    ) as error:
        print(f"workstream shipyard profile refused: {error}", file=sys.stderr)
        return 2
    print(json.dumps({
        "checkpoint_id": profile["checkpoint"]["checkpoint_id"],
        "context_url": profile["continuation_bootstrap"]["context_url"],
        "output": str(output),
        "provider": profile["provider"]["provider_id"],
        "repository": profile["worktree"]["repository"],
        "workstream_id": token,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
