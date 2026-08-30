#!/usr/bin/env python3
"""Append one revision-fenced material event to an exact workstream child."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Callable

from workstream_child_target import add_target_arguments, authenticate_child_target
from workstream_delta import Delta, event_id_for
from workstream_linear import HttpGraphQLClient, LinearTransportError
from workstream_linear_events import LinearCommentEventAdapter
from workstream_child_proposal import (
    activated_comments, append_proposal, build_proposal,
)
from workstream_linear_events import reduce_event_comments
from workstream_linear_projection import child_mutation_authorizations_from_comments


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    add_target_arguments(value)
    value.add_argument("--kind", required=True)
    value.add_argument(
        "--source", required=True,
        choices=("user_turn", "agent_discovery", "checkpoint", "system"),
    )
    value.add_argument("--payload-json", required=True)
    value.add_argument("--expected-revision", required=True, type=int)
    value.add_argument("--created-at", required=True)
    value.add_argument("--event-id")
    return value


def run(
    argv: list[str], *,
    client_factory: Callable[[str], Any] = HttpGraphQLClient,
) -> dict[str, Any]:
    args = parser().parse_args(argv)
    if args.expected_revision < 0:
        raise ValueError("expected child revision must be non-negative")
    try:
        payload = json.loads(args.payload_json)
    except json.JSONDecodeError as error:
        raise ValueError("child event payload must be valid JSON") from error
    if not isinstance(payload, dict):
        raise ValueError("child event payload must be a JSON object")
    event_id = args.event_id or event_id_for(
        args.child_workstream_id.upper(), args.kind, payload,
        args.expected_revision, source=args.source,
    )
    delta = Delta(
        event_id, args.child_workstream_id.upper(), args.kind, args.source,
        payload, args.expected_revision, args.created_at,
    )
    proposal = build_proposal(
        "event", delta.__dict__,
        child_workstream_id=args.child_workstream_id.upper(),
        child_issue_id=args.child_issue_id.lower(),
        plan_revision=args.plan_revision,
    )
    target = authenticate_child_target(
        args, proposal_id=proposal["proposal_id"],
        client_factory=client_factory,
    )
    proposal_receipt = append_proposal(target["client"], proposal)
    selected = target["generation_authority"]
    generation = {
        key: selected[key] for key in (
            "plan_revision", "description_plan_revision",
            "transition_tip_event_id", "activation_epoch", "authority_origin",
            "workstream_id", "authority", "source",
        )
    }
    authorization = target["projection"].reserve_child_mutation(
        proposal=proposal, proposal_remote_id=proposal_receipt["remote_id"],
        child_identity=target["child_identity"], generation_authority=generation,
        scope_event_id=selected["scope_event_id"],
        scope_value_sha256=selected["scope_value_sha256"],
        repository_owner=selected["child_repository_owner"],
        child_origin=selected["child_origin"],
        expected_projection_revision=selected["projection_revision"],
    )
    comments = LinearCommentEventAdapter(
        target["client"], issue_id=target["child_workstream_id"],
        plan_revision=target["plan_revision"], root_issue_id=target["child_issue_id"],
        **target["route"],
    ).comments()
    authorizations = child_mutation_authorizations_from_comments(
        target["projection"]._comments(),
        workstream_id=target["root_workstream_id"],
        description_plan_revision=selected["description_plan_revision"],
        authenticated_route={**target["route"], "root_issue_id": target["root_issue_id"]},
    )
    active = activated_comments(
        comments, authorizations,
        child_workstream_id=target["child_workstream_id"],
        child_issue_id=target["child_issue_id"],
    )
    state = reduce_event_comments(active, workstream_id=target["child_workstream_id"])
    remote_id = state.remote_ids[event_id]
    return {
        "schema_version": 1,
        "root_workstream_id": target["root_workstream_id"],
        "child_workstream_id": target["child_workstream_id"],
        "child_issue_id": target["child_issue_id"],
        "plan_revision": target["plan_revision"],
        "generation_authority": target["generation_authority"],
        "proposal": proposal_receipt, "authorization": authorization,
        "receipt": {
            "event_id": event_id, "revision": list(state.events).index(delta) + 1,
            "remote_id": remote_id,
        },
    }


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(sys.argv[1:] if argv is None else argv)
    except (LinearTransportError, OSError, TimeoutError, ValueError) as error:
        print(f"workstream child event failed: {error}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
