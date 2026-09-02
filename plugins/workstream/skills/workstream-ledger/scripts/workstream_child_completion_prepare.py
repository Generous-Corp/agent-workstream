#!/usr/bin/env python3
"""Prepare a read-only evidence or terminal-closure projection for one child."""

from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
import sys
from typing import Any, Callable

from workstream_child_closure import canonical_digest, terminal_child_readback
from workstream_child_dependencies import (
    ChildDependencyError, LinearChildDependencyAdapter,
)
from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_evidence import evidence_errors
from workstream_linear import (
    HttpGraphQLClient, LinearGraphQLTransport, LinearTransportError,
    resolve_authenticated_issue_route,
)
from workstream_linear_events import LinearCommentEventAdapter
from workstream_linear_projection import LinearProjectionAdapter, LinearProjectionError
from workstream_plan import plan_payload
from workstream_projection import (
    _active_heads, _completed_owned_missing_closures,
    bind_projection_plan_generation, prepare_terminal_child_repairs,
    canonical_source_diagnostic_fence, projection_generation_source_binding,
    projection_review_contract, stable_live_readback,
    validate_canonical_source_readback,
)
from workstream_relation_readback import read_relation_targets
from workstream_resume import (
    ResumeError, add_live_child_material_history, add_material_history,
    compact_context, extract_token,
)
from workstream_scope import canonical_repository, repository_key


class ChildCompletionPrepareError(RuntimeError):
    pass


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("root_workstream_id", metavar="GEN-123")
    value.add_argument("--child", required=True, metavar="GEN-124")
    value.add_argument("--evidence-contract", required=True, metavar="JSON")
    value.add_argument("--plan-source", required=True)
    value.add_argument("--plan-identity")
    value.add_argument("--config")
    value.add_argument("--workspace-id")
    value.add_argument("--team-id")
    value.add_argument("--project-id")
    value.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    return value


def _desired_active_projection(state: Any) -> list[dict[str, Any]]:
    return [
        {"kind": kind, "key": key, "value": deepcopy(event["value"])}
        for (kind, key), event in sorted(_active_heads(state).items())
        if kind != "disposition"
    ]


def _one_child(snapshot: dict[str, Any], child_token: str) -> dict[str, Any]:
    matches = [
        child for child in snapshot.get("children", [])
        if str(child.get("identifier", "")).upper() == child_token
    ]
    if len(matches) != 1:
        raise ChildCompletionPrepareError(
            f"child_identity_not_unique:{child_token}"
        )
    return matches[0]


def _validate_contract_owner(
    contract: dict[str, Any], *, child_token: str, plan_revision: str,
    scope: dict[str, Any],
) -> None:
    errors = evidence_errors(contract)
    if errors:
        raise ChildCompletionPrepareError(
            "evidence_contract_invalid:" + ",".join(errors)
        )
    if contract.get("owning_child") != child_token:
        raise ChildCompletionPrepareError("evidence_contract_child_mismatch")
    if contract.get("plan_revision") != plan_revision:
        raise ChildCompletionPrepareError("evidence_contract_plan_mismatch")
    owner = (scope.get("child_ownership") or {}).get(child_token)
    if not isinstance(owner, str) or owner != contract.get("repository_key"):
        raise ChildCompletionPrepareError("evidence_contract_owner_mismatch")
    repositories = [
        repository for repository in scope.get("repositories", [])
        if repository_key(repository) == owner
    ]
    if len(repositories) != 1:
        raise ChildCompletionPrepareError("evidence_contract_repository_ambiguous")
    repository = repositories[0]
    if (
        repository.get("exact_head") != contract.get("exact_head")
        or canonical_repository(str(repository.get("slug", "")))
        != contract.get("repository")
    ):
        raise ChildCompletionPrepareError("evidence_contract_repository_mismatch")


def prepare_child_completion(
    snapshot: dict[str, Any], state: Any, *, root_token: str,
    child_token: str, evidence_contract: dict[str, Any],
    authenticated_source: dict[str, Any], authenticated_route: dict[str, str],
) -> dict[str, Any]:
    """Return a complete reviewed manifest without mutating any remote surface."""
    plan_revision = authenticated_source.get("sha256")
    if snapshot.get("root", {}).get("plan_revision") != plan_revision:
        raise ChildCompletionPrepareError("active_generation_source_mismatch")
    if snapshot.get("authenticated_route") != authenticated_route:
        raise ChildCompletionPrepareError("authenticated_route_mismatch")
    if snapshot.get("authenticated_source") != authenticated_source:
        raise ChildCompletionPrepareError("authenticated_source_mismatch")

    active = _active_heads(state)
    source = active.get(("source", "root"))
    scope_event = active.get(("scope", "root"))
    if source is None or source.get("value") != {
        "identity": authenticated_source.get("identity"),
        "sha256": plan_revision,
    }:
        raise ChildCompletionPrepareError("active_projection_source_mismatch")
    if scope_event is None or not isinstance(scope_event.get("value"), dict):
        raise ChildCompletionPrepareError("active_projection_scope_missing")
    scope = scope_event["value"]
    linear = scope.get("linear") or {}
    if any(
        linear.get(field) != authenticated_route.get(field)
        for field in ("workspace_id", "team_id", "project_id", "root_issue_id")
    ):
        raise ChildCompletionPrepareError("active_projection_route_mismatch")

    child = _one_child(snapshot, child_token)
    if (
        (child.get("parent") or {}).get("id") != authenticated_route["root_issue_id"]
        or (child.get("team") or {}).get("id") != authenticated_route["team_id"]
        or (child.get("project") or {}).get("id") != authenticated_route["project_id"]
        or ((child.get("team") or {}).get("organization") or {}).get("id")
        != authenticated_route["workspace_id"]
    ):
        raise ChildCompletionPrepareError("child_native_route_mismatch")
    _validate_contract_owner(
        evidence_contract, child_token=child_token,
        plan_revision=str(plan_revision), scope=scope,
    )

    missing_closures = _completed_owned_missing_closures(snapshot, scope, active)
    if missing_closures - {child_token}:
        raise ChildCompletionPrepareError(
            "other_terminal_child_repairs_required:"
            + ",".join(sorted(missing_closures - {child_token}))
        )
    desired = _desired_active_projection(state)
    review_contract = projection_review_contract(state)
    manifest: dict[str, Any] = {
        "projection": desired, "retirements": [], **review_contract,
    }
    evidence_identity = ("evidence_contract", evidence_contract["slice_id"])
    existing_evidence = active.get(evidence_identity)
    status_type = str(child.get("status_type") or "").lower()
    if not status_type and isinstance(child.get("state"), dict):
        status_type = str(child["state"].get("type") or "").lower()

    if existing_evidence is None:
        manifest["projection"].append({
            "kind": "evidence_contract", "key": evidence_contract["slice_id"],
            "value": deepcopy(evidence_contract),
        })
        manifest["projection"].sort(key=lambda item: (item["kind"], item["key"]))
        phase = "evidence_projection_required"
    elif existing_evidence.get("value") != evidence_contract:
        raise ChildCompletionPrepareError("active_evidence_contract_conflict")
    elif status_type == "canceled":
        raise ChildCompletionPrepareError("child_canceled_completion_forbidden")
    elif status_type != "completed":
        phase = "native_transition_required"
    elif ("child_closure", child_token) in active:
        phase = "complete"
    else:
        readback = terminal_child_readback(child)
        approved = [
            {
                "key": key, "event_id": event["event_id"],
                "value_sha256": canonical_digest(event["value"]),
            }
            for (kind, key), event in sorted(active.items())
            if kind == "evidence_contract"
            and event["value"].get("owning_child") == child_token
        ]
        manifest["terminal_child_repairs"] = [{
            "child_identifier": child_token,
            "child_issue_id": readback["child_issue_id"],
            "expected_child_readback_sha256": canonical_digest(readback),
            "expected_assignee_id": readback["assignee_id"],
            "approved_evidence_heads": approved,
        }]
        manifest = prepare_terminal_child_repairs(manifest, snapshot, state)
        phase = "closure_projection_required"

    return {
        "schema_version": 1,
        "operation_status": phase,
        "root_workstream_id": root_token,
        "child_workstream_id": child_token,
        "child_issue_id": child.get("id"),
        "plan_revision": plan_revision,
        "native_state": {
            "id": child.get("state_id") or (child.get("state") or {}).get("id"),
            "name": child.get("status") or (child.get("state") or {}).get("name"),
            "type": status_type,
        },
        "projection_manifest": manifest,
    }


def run(
    argv: list[str], *, client_factory: Callable[..., Any] = HttpGraphQLClient,
) -> dict[str, Any]:
    args = parser().parse_args(argv)
    root_token = extract_token(args.root_workstream_id)
    child_token = extract_token(args.child)
    if root_token == child_token:
        raise ChildCompletionPrepareError("root_and_child_must_differ")
    try:
        evidence_contract = json.loads(
            Path(args.evidence_contract).read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as error:
        raise ChildCompletionPrepareError("evidence_contract_invalid_json") from error
    if not isinstance(evidence_contract, dict):
        raise ChildCompletionPrepareError("evidence_contract_must_be_object")
    authenticated_source = plan_payload(
        args.plan_source, args.plan_identity,
    )["source"]
    api_key = load_linear_api_key()
    if not api_key:
        raise ChildCompletionPrepareError("linear_auth_unavailable")
    client = client_factory(api_key, args.linear_endpoint)
    declared_route, _ = resolve_linear_route(
        config_path=args.config, workspace_id=args.workspace_id,
        team_id=args.team_id, project_id=args.project_id,
    )
    route = resolve_authenticated_issue_route(client, root_token, declared_route)
    transport = LinearGraphQLTransport(
        client, workspace_id=route["workspace_id"], team_id=route["team_id"],
        project_id=route["project_id"],
    )
    comments_adapter = LinearCommentEventAdapter(
        client, issue_id=root_token, workspace_id=route["workspace_id"],
        team_id=route["team_id"], project_id=route["project_id"],
    )
    adapter = LinearProjectionAdapter(
        client, issue_id=root_token, workstream_id=root_token,
        plan_revision=authenticated_source["sha256"],
        workspace_id=route["workspace_id"], team_id=route["team_id"],
        project_id=route["project_id"], root_issue_id=route["root_issue_id"],
    )
    graph, comments = stable_live_readback(
        transport, comments_adapter, root_token, include_description=True,
        include_child_comments=True,
    )
    description_fence = canonical_source_diagnostic_fence(
        graph["root"].get("description")
    )
    selector_revision = graph["root"].get("plan_revision")
    binding = projection_generation_source_binding(
        comments, workstream_id=root_token,
        description_plan_revision=selector_revision,
        requested_plan_revision=authenticated_source["sha256"],
        authenticated_route=route,
    )
    if binding["mode"] == "inactive_candidate":
        raise ChildCompletionPrepareError("active_generation_source_mismatch")
    graph = bind_projection_plan_generation(
        graph, comments, workstream_id=root_token,
        requested_plan_revision=authenticated_source["sha256"],
        authenticated_route=route,
    )
    graph = add_live_child_material_history(
        graph, authenticated_route=route, root_comments=comments,
        proposal_plan_revision=(
            binding["selected"] or {
                "plan_revision": authenticated_source["sha256"],
            }
        )["plan_revision"],
    )

    def reread() -> tuple[dict[str, Any], list[dict[str, Any]]]:
        reread_graph, reread_comments = stable_live_readback(
            transport, comments_adapter, root_token, include_description=True,
            include_child_comments=True,
        )
        validate_canonical_source_readback(
            reread_graph["root"].get("description"), description_fence,
        )
        return reread_graph, reread_comments

    dependency_adapter = LinearChildDependencyAdapter(
        client, workspace_id=route["workspace_id"], team_id=route["team_id"],
        project_id=route["project_id"], root_issue_id=route["root_issue_id"],
        root_identifier=root_token,
        plan_revision=authenticated_source["sha256"],
    )
    graph["dependency_graph"] = dependency_adapter.read_authorized_graph_for_snapshot(
        graph, comments, generation_selector_plan_revision=selector_revision,
        reread=reread,
    )
    snapshot = add_material_history(
        graph, comments, root_token, authenticated_route=route,
        authenticated_source=authenticated_source,
        relation_target_resolver=lambda relations: read_relation_targets(
            client, relations,
        ),
    )
    active = _active_heads(adapter.state())
    scope_event = active.get(("scope", "root"))
    missing_closures = _completed_owned_missing_closures(
        snapshot, (scope_event or {"value": {}})["value"], active,
    )
    compact_context(
        snapshot, root_token, require_projection_authority=True,
        require_dependency_graph=True,
        expected_missing_terminal_closures=frozenset(missing_closures),
    )
    return prepare_child_completion(
        snapshot, adapter.state(), root_token=root_token,
        child_token=child_token, evidence_contract=evidence_contract,
        authenticated_source=authenticated_source, authenticated_route=route,
    )


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(sys.argv[1:] if argv is None else argv)
    except (
        ChildCompletionPrepareError, ChildDependencyError, LinearProjectionError,
        LinearTransportError, OSError, ResumeError, ValueError,
    ) as error:
        print(f"workstream child completion prepare refused: {error}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
