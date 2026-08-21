#!/usr/bin/env python3
"""Validate and compact a Linear-backed workstream snapshot for recovery.

The transport that obtains the snapshot may be Linear MCP, a future CLI, or a
repository-specific adapter. This command is deliberately transport-neutral:
it validates the durable join and refuses ambiguous/stale/incomplete input
before an agent edits anything.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any

from workstream_linear import HttpGraphQLClient, LinearGraphQLTransport, LinearTransportError
from workstream_choices import ChoiceError, reduce_choices
from workstream_evidence import evidence_errors
from workstream_scope import repository_key, ScopeError, validate_relations, validate_scope


TOKEN = re.compile(r"\b[A-Z][A-Z0-9]*-\d+\b", re.I)
TERMINAL = {"done", "cancelled", "canceled", "superseded"}


class ResumeError(ValueError):
    pass


def extract_token(value: str) -> str:
    """Resolve one distinct workstream token from a token, URL, or tab title."""
    tokens = {match.group(0).upper() for match in TOKEN.finditer(value or "")}
    if not tokens:
        raise ResumeError("missing_workstream_token")
    if len(tokens) != 1:
        raise ResumeError("multiple_workstream_tokens:" + ",".join(sorted(tokens)))
    return next(iter(tokens))


def validate_snapshot(snapshot: dict[str, Any], token: str | None = None) -> dict[str, Any]:
    root = snapshot.get("root")
    if not isinstance(root, dict):
        raise ResumeError("missing root")
    identifier = root.get("identifier") or root.get("id")
    if not isinstance(identifier, str) or not TOKEN.fullmatch(identifier.upper()):
        raise ResumeError("root must contain one Linear issue identifier")
    if token and identifier.upper() != extract_token(token):
        raise ResumeError("token/root mismatch")
    for field in ("url", "plan_revision", "revision"):
        if field not in root or root[field] in (None, ""):
            raise ResumeError(f"root missing {field}")
    if not isinstance(root["revision"], int) or root["revision"] < 0:
        raise ResumeError("root revision must be a non-negative integer")
    if str(root.get("status", "")).lower() not in TERMINAL and not root.get("next_action"):
        raise ResumeError("nonterminal root missing next_action")
    children = snapshot.get("children")
    if not isinstance(children, list):
        raise ResumeError("children must be a list")
    keys: set[str] = set()
    for child in children:
        if not isinstance(child, dict) or not child.get("identifier") or not child.get("title"):
            raise ResumeError("every child needs identifier and title")
        key = str(child["identifier"]).upper()
        if key in keys:
            raise ResumeError(f"duplicate child:{key}")
        keys.add(key)
        if str(child.get("status", "")).lower() not in TERMINAL and not child.get("next_action"):
            raise ResumeError(f"nonterminal child missing next_action:{key}")
    choice_events = snapshot.get("choice_events", [])
    if not isinstance(choice_events, list):
        raise ResumeError("choice_events must be a list")
    try:
        choice_view = reduce_choices(choice_events)
        scope = snapshot.get("scope")
        if scope is not None:
            validate_scope(scope, root_id=identifier.upper(), child_ids=keys)
        relations = snapshot.get("relations", [])
        validate_relations(
            relations, root_id=identifier.upper(),
            workspace_id=scope.get("linear", {}).get("workspace_id") if scope else None,
            root_issue_id=scope.get("linear", {}).get("root_issue_id") if scope else None,
        )
        evidence_contracts = snapshot.get("evidence_contracts", [])
        if not isinstance(evidence_contracts, list):
            raise ResumeError("evidence_contracts must be a list")
        for index, contract in enumerate(evidence_contracts):
            errors = evidence_errors(contract)
            if errors:
                raise ResumeError(f"invalid_evidence_contract:{index}:" + ",".join(errors))
            if contract.get("plan_revision") != root["plan_revision"]:
                raise ResumeError(f"evidence_plan_drift:{index}")
            if contract.get("owning_child") not in keys:
                raise ResumeError(f"evidence_owner_missing:{index}")
            if scope is not None:
                owned_key = scope["child_ownership"][contract["owning_child"]]
                if contract.get("repository_key") != owned_key:
                    raise ResumeError(f"evidence_repository_mismatch:{index}")
                scoped_repository = next(
                    item for item in scope["repositories"] if repository_key(item) == owned_key
                )
                if contract.get("repository") not in [scoped_repository["slug"], *scoped_repository.get("aliases", [])]:
                    raise ResumeError(f"evidence_repository_route_unknown:{index}")
                if contract.get("exact_head") != scoped_repository["exact_head"]:
                    raise ResumeError(f"evidence_head_mismatch:{index}")
        for choice_id, view in choice_view.items():
            event = view["record"]
            if event["workstream_id"] != identifier.upper():
                raise ResumeError(f"choice_workstream_mismatch:{choice_id}")
            if event["owning_child"] not in keys:
                raise ResumeError(f"choice_owner_missing:{choice_id}")
            if scope is not None:
                if event["namespace"] != scope["namespace"]:
                    raise ResumeError(f"choice_namespace_mismatch:{choice_id}")
                owned_key = scope["child_ownership"][event["owning_child"]]
                if owned_key != event["repository_key"]:
                    raise ResumeError(f"choice_repository_mismatch:{choice_id}")
                scoped_repository = next(
                    item for item in scope["repositories"]
                    if repository_key(item) == owned_key
                )
                if event["repository"] not in [scoped_repository["slug"], *scoped_repository.get("aliases", [])]:
                    raise ResumeError(f"choice_repository_route_unknown:{choice_id}")
        availability = {
            field: "available" if field in snapshot and snapshot.get(field) is not None
            else "transport_unimplemented"
            for field in ("scope", "relations", "choice_events", "evidence_contracts")
        }
    except (ChoiceError, ScopeError) as error:
        raise ResumeError(str(error)) from error
    return {"root": root, "children": children, "decisions": snapshot.get("decisions", []),
            "choice_events": choice_events, "scope": scope,
            "relations": relations, "evidence_contracts": evidence_contracts,
            "surface_availability": availability,
            "provenance": snapshot.get("provenance", []),
            "source": snapshot.get("source")}


def compact_context(
    snapshot: dict[str, Any], token: str, max_bytes: int = 16 * 1024,
    max_items: int = 100,
) -> dict[str, Any]:
    normalized_token = extract_token(token)
    clean = validate_snapshot(snapshot, normalized_token)
    root = clean["root"]
    children = [
        child for child in clean["children"]
        if str(child.get("status", "")).lower() not in TERMINAL
    ]
    context = {
        "workstream_id": root["identifier"].upper(),
        "context_url": root["url"],
        "plan_revision": root["plan_revision"],
        "root_revision": root["revision"],
        "status": root.get("status"),
        "next_action": root.get("next_action"),
        "children": children,
        "decisions": clean["decisions"],
        "choice_events": clean["choice_events"],
        "scope": clean["scope"],
        "relations": clean["relations"],
        "evidence_contracts": clean["evidence_contracts"],
        "surface_availability": clean["surface_availability"],
        "provenance": clean["provenance"],
        "source": clean.get("source"),
    }
    item_count = sum(
        len(value) for value in (
            context["children"], context["decisions"], context["choice_events"],
            context["relations"], context["provenance"],
            context["evidence_contracts"],
        )
    )
    if max_items < 0 or item_count > max_items:
        raise ResumeError(f"resume_context_over_item_budget:{item_count}>{max_items}")
    encoded = json.dumps(context, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    if len(encoded) > max_bytes:
        raise ResumeError(f"resume_context_over_budget:{len(encoded)}>{max_bytes}")
    return context


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("token", help="stable Linear root issue identifier")
    parser.add_argument("snapshot", nargs="?", help="JSON path or - for a Linear snapshot")
    parser.add_argument("--linear-team-id", help="Fetch the root and children from Linear instead of a local snapshot")
    parser.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    parser.add_argument("--max-bytes", type=int, default=16 * 1024)
    parser.add_argument("--max-items", type=int, default=100)
    args = parser.parse_args()
    try:
        token = extract_token(args.token)
        if args.snapshot is None:
            if not args.linear_team_id:
                raise ResumeError("snapshot or --linear-team-id is required")
            api_key = os.environ.get("LINEAR_API_KEY")
            if not api_key:
                raise ResumeError("LINEAR_API_KEY is required for live Linear resume")
            transport = LinearGraphQLTransport(HttpGraphQLClient(api_key, args.linear_endpoint), team_id=args.linear_team_id)
            snapshot = transport.snapshot_for_root(token)
        else:
            raw = sys.stdin.read() if args.snapshot == "-" else open(args.snapshot, encoding="utf-8").read()
            snapshot = json.loads(raw)
        output = compact_context(snapshot, token, args.max_bytes, args.max_items)
    except (OSError, json.JSONDecodeError, ResumeError, LinearTransportError, ValueError) as error:
        print(f"workstream resume refused: {error}", file=sys.stderr)
        return 2
    json.dump(output, sys.stdout, ensure_ascii=False, sort_keys=True, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
