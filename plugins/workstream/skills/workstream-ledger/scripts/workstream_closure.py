#!/usr/bin/env python3
"""Bounded, model-free adversarial closure review for a workstream snapshot."""

from __future__ import annotations

from typing import Any

from workstream_choices import (
    ChoiceError, closure_blockers, retired_acceptance_criteria,
)
from workstream_evidence import evidence_errors
from workstream_scope import (
    is_full_oid, repository_key, ScopeError, validate_relation_graph,
    validate_relations, validate_scope,
)
from workstream_projection_history import (
    closure_bound_historical_evidence, ProjectionHistoryError,
)


TERMINAL = {"done", "cancelled", "canceled", "superseded"}


def review(snapshot: dict[str, Any], *, expected_plan_revision: str,
          criteria: list[str], evidence: dict[str, Any],
          excluded: list[dict[str, str]] | None = None,
          semantic_review_invoked: bool = False,
          semantic_review_passed: bool = False,
          required_child_ids: set[str] | None = None,
          choice_events: list[dict[str, Any]] | None = None,
          evidence_contracts: list[dict[str, Any]] | None = None,
          exact_head: str | None = None,
          repository_heads: dict[str, str] | None = None) -> dict[str, Any]:
    """Return deterministic errors or a durable receipt payload.

    The function only enumerates evidence and consistency.  It never calls a
    model and never changes a status; semantic ambiguity remains explicit in
    the receipt for a bounded follow-up review.
    """
    errors: list[str] = []
    root = snapshot.get("root") or {}
    availability = snapshot.get("surface_availability")
    if not isinstance(availability, dict):
        availability = {
            field: "available" if field in snapshot else "transport_unimplemented"
            for field in ("scope", "relations", "choice_events", "evidence_contracts")
        }
    expected_surface_types = {
        "scope": dict,
        "relations": list,
        "choice_events": list,
        "evidence_contracts": list,
    }
    for surface in ("scope", "relations", "choice_events", "evidence_contracts"):
        if availability.get(surface) != "available":
            errors.append(f"transport_unimplemented:{surface}")
        elif surface not in snapshot or snapshot.get(surface) is None:
            errors.append(f"durable_surface_missing:{surface}")
        elif not isinstance(snapshot[surface], expected_surface_types[surface]):
            errors.append(f"durable_surface_malformed:{surface}")
    if root.get("plan_revision") != expected_plan_revision:
        errors.append("plan_sync_required")
    children = snapshot.get("children") or []
    identifiers = [str(child.get("identifier", "")) for child in children]
    required = set(identifiers) if required_child_ids is None else set(required_child_ids)
    if len(identifiers) != len(set(identifiers)):
        errors.append("duplicate_child")
    errors.extend(
        f"missing_child:{identifier}"
        for identifier in sorted(required - set(identifiers))
    )
    for child in children:
        status = str(child.get("status", "")).lower()
        identifier = str(child.get("identifier", ""))
        if identifier in required and status not in TERMINAL:
            errors.append(f"required_child_open:{identifier}")
        if status not in TERMINAL and not child.get("owner"):
            errors.append(f"unowned_nonterminal:{identifier}")
        if status not in TERMINAL and not child.get("next_action"):
            errors.append(f"missing_next_action:{identifier}")
        if status == "blocked" and not child.get("review_condition"):
            errors.append(f"blocked_without_review_condition:{identifier}")
    for criterion in criteria:
        item = evidence.get(criterion)
        if not item or not item.get("satisfied"):
            errors.append(f"criterion_not_proven:{criterion}")
    for key in ("decisions", "followups", "prs", "landing_receipts", "tests", "artifacts"):
        # Read old snapshots without making a particular landing controller part
        # of the portable contract.
        if key == "landing_receipts" and "shipyard_receipts" in evidence:
            continue
        if key not in evidence:
            errors.append(f"missing_evidence_category:{key}")
    if semantic_review_passed and not semantic_review_invoked:
        errors.append("semantic_review_result_without_invocation")
    choice_events = snapshot.get("choice_events") if choice_events is None else choice_events
    evidence_contracts = snapshot.get("evidence_contracts") if evidence_contracts is None else evidence_contracts
    if not isinstance(choice_events, list):
        errors.append("durable_surface_malformed:choice_events")
    if not isinstance(evidence_contracts, list):
        errors.append("durable_surface_malformed:evidence_contracts")
    scope = snapshot.get("scope")
    relations = snapshot.get("relations")
    projection_events = snapshot.get("projection_events")
    historical_evidence_event_ids: frozenset[str] = frozenset()
    active_evidence_events: dict[str, dict[str, Any]] = {}
    if isinstance(projection_events, list):
        for event in projection_events:
            identity = (event.get("kind"), event.get("key"))
            if identity[0] != "evidence_contract":
                continue
            if event.get("value") == {"_projection_tombstone": True}:
                active_evidence_events.pop(str(identity[1]), None)
            else:
                active_evidence_events[str(identity[1])] = event
    if isinstance(scope, dict) and isinstance(projection_events, list):
        try:
            historical_evidence_event_ids = closure_bound_historical_evidence(
                projection_events, scope,
            )
        except ProjectionHistoryError as error:
            errors.append(str(error))
    if isinstance(choice_events, list):
        if not exact_head and repository_heads is None:
            errors.append("choice_reconciliation_missing_exact_head")
        else:
            try:
                errors.extend(closure_blockers(
                    choice_events, plan_revision=expected_plan_revision,
                    exact_head=exact_head, repository_heads=repository_heads,
                    workstream_id=root.get("identifier"), child_ids=set(identifiers),
                ))
            except ChoiceError as error:
                errors.append(f"invalid_choice_history:{error}")
        try:
            retired_criteria = retired_acceptance_criteria(choice_events)
            errors.extend(
                f"retired_choice_criterion:{retired_criteria[criterion]}:{criterion}"
                for criterion in criteria if criterion in retired_criteria
            )
        except ChoiceError as error:
            errors.append(f"invalid_choice_history:{error}")
    for index, contract in enumerate(evidence_contracts if isinstance(evidence_contracts, list) else []):
        errors.extend(
            f"evidence_contract:{index}:{error}"
            for error in evidence_errors(contract)
        )
    if isinstance(scope, dict):
        try:
            validate_scope(scope, root_id=str(root.get("identifier", "")), child_ids=set(identifiers))
            validate_relations(
                relations if isinstance(relations, list) else [],
                root_id=str(root.get("identifier", "")),
                workspace_id=scope["linear"]["workspace_id"],
                root_issue_id=scope["linear"]["root_issue_id"],
            )
            if relations:
                validate_relation_graph(
                    relations, root_id=str(root.get("identifier", "")),
                    workspace_id=scope["linear"]["workspace_id"],
                    root_issue_id=scope["linear"]["root_issue_id"],
                    resolve_target=snapshot.get("relation_targets") or {},
                )
                errors.extend(
                    f"blocked_by:{relation['target']['identifier']}"
                    for relation in relations
                    if relation["type"] == "blocked_by"
                )
            scoped_repositories = {repository_key(item): item for item in scope["repositories"]}
            if repository_heads is None:
                if len(scoped_repositories) == 1 and exact_head:
                    repository_heads = {next(iter(scoped_repositories)): exact_head}
                else:
                    repository_heads = {}
            if set(repository_heads) != set(scoped_repositories):
                errors.append("repository_head_keyset_mismatch")
            for key, head in repository_heads.items():
                if not is_full_oid(head) or key not in scoped_repositories:
                    errors.append(f"invalid_repository_head:{key}")
                elif scoped_repositories[key].get("exact_head") != head:
                    errors.append(f"scope_repository_head_mismatch:{key}")
            for event in choice_events if isinstance(choice_events, list) else []:
                key = event.get("repository_key")
                repository = scoped_repositories.get(key)
                if repository is None:
                    errors.append(f"choice_repository_key_unknown:{event.get('choice_id')}")
                elif event.get("repository") not in [repository["slug"], *repository.get("aliases", [])]:
                    errors.append(f"choice_repository_route_unknown:{event.get('choice_id')}")
            contracts_by_child: dict[str, list[dict[str, Any]]] = {}
            slice_ids: set[str] = set()
            for contract in evidence_contracts if isinstance(evidence_contracts, list) else []:
                contracts_by_child.setdefault(str(contract.get("owning_child")), []).append(contract)
                child = str(contract.get("owning_child"))
                slice_id = str(contract.get("slice_id"))
                if slice_id in slice_ids:
                    errors.append(f"duplicate_evidence_slice:{slice_id}")
                slice_ids.add(slice_id)
                if contract.get("plan_revision") != expected_plan_revision:
                    errors.append(f"evidence_plan_drift:{slice_id}")
                expected_key = scope["child_ownership"].get(child)
                if contract.get("repository_key") != expected_key:
                    errors.append(f"evidence_repository_mismatch:{child}")
                evidence_event = active_evidence_events.get(slice_id)
                historically_closed = (
                    evidence_event is not None
                    and evidence_event.get("event_id")
                    in historical_evidence_event_ids
                )
                if (
                    contract.get("repository_key") == expected_key
                    and repository_heads.get(expected_key)
                    != contract.get("exact_head")
                    and not historically_closed
                ):
                    errors.append(f"evidence_head_mismatch:{child}")
            for child in sorted(required):
                if not contracts_by_child.get(child):
                    errors.append(f"missing_evidence_contract:{child}")
        except (ScopeError, KeyError, TypeError) as error:
            errors.append(f"invalid_scope_or_relations:{error}")
    if str(root.get("status", "")).lower() == "done" and any(
        str(child.get("status", "")).lower() not in TERMINAL for child in children
    ):
        errors.append("done_with_open_children")
    if errors:
        return {"ok": False, "errors": sorted(set(errors)), "receipt": None}
    receipt = {
        "criteria_checked": criteria,
        "children_checked": identifiers,
        "evidence_categories_checked": ["decisions", "followups", "prs", "landing_receipts", "tests", "artifacts"],
        "excluded": excluded or [],
        "deterministic_checks_passed": True,
        "semantic_review_invoked": semantic_review_invoked,
        "semantic_review_passed": semantic_review_passed,
        "resume_token": root.get("identifier"),
        "context_url": root.get("url"),
        "plan_revision": root.get("plan_revision"),
        "root_revision": root.get("revision"),
        "final_disposition": (
            "Done"
            if semantic_review_invoked and semantic_review_passed and not excluded
            else "Landed — acceptance review required"
        ),
    }
    return {"ok": True, "errors": [], "receipt": receipt}
