#!/usr/bin/env python3
"""Activate one exact inert child proposal recovered from its bounded handle."""

from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any, Callable

from workstream_child_proposal import _comments, proposal_index
from workstream_child_target import add_target_arguments, authenticate_child_target
from workstream_linear import HttpGraphQLClient, LinearTransportError


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    add_target_arguments(value)
    value.add_argument("--proposal-id", required=True)
    value.add_argument("--proposal-remote-id", required=True)
    return value


def run(
    argv: list[str], *, client_factory: Callable[[str], Any] = HttpGraphQLClient,
) -> dict[str, Any]:
    args = parser().parse_args(argv)
    if re.fullmatch(r"wscp_[0-9a-f]{32}", args.proposal_id) is None:
        raise ValueError("invalid child proposal ID")
    target = authenticate_child_target(
        args, proposal_id=args.proposal_id, client_factory=client_factory,
    )
    found = proposal_index(_comments(
        target["client"], target["child_workstream_id"],
    )).get(args.proposal_id)
    if found is None:
        raise LinearTransportError("child_proposal_not_found")
    proposal, comment = found
    if comment.get("id") != args.proposal_remote_id:
        raise LinearTransportError("child_proposal_remote_id_mismatch")
    if (
        proposal["child_workstream_id"] != target["child_workstream_id"]
        or proposal["child_issue_id"] != target["child_issue_id"]
        or proposal["plan_revision"] != target["plan_revision"]
    ):
        raise LinearTransportError("foreign_child_proposal")
    selected = target["generation_authority"]
    generation = {
        key: selected[key] for key in (
            "plan_revision", "description_plan_revision",
            "transition_tip_event_id", "activation_epoch", "authority_origin",
            "workstream_id", "authority", "source",
        )
    }
    authorization = target["projection"].reserve_child_mutation(
        proposal=proposal, proposal_remote_id=args.proposal_remote_id,
        child_identity=target["child_identity"], generation_authority=generation,
        scope_event_id=selected["scope_event_id"],
        scope_value_sha256=selected["scope_value_sha256"],
        repository_owner=selected["child_repository_owner"],
        child_origin=selected["child_origin"],
        expected_projection_revision=selected["projection_revision"],
    )
    return {
        "schema_version": 1,
        "root_workstream_id": target["root_workstream_id"],
        "child_workstream_id": target["child_workstream_id"],
        "child_issue_id": target["child_issue_id"],
        "plan_revision": target["plan_revision"],
        "proposal": {
            "proposal_id": proposal["proposal_id"],
            "proposal_remote_id": args.proposal_remote_id,
            "kind": proposal["kind"],
            "record_sha256": proposal["record_sha256"],
        },
        "authorization": authorization,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(sys.argv[1:] if argv is None else argv)
    except (LinearTransportError, OSError, TimeoutError, ValueError) as error:
        print(f"workstream child proposal activation failed: {error}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
