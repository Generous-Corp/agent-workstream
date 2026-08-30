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
    target = authenticate_child_target(args, client_factory=client_factory)
    event_id = args.event_id or event_id_for(
        target["child_workstream_id"], args.kind, payload,
        args.expected_revision, source=args.source,
    )
    delta = Delta(
        event_id, target["child_workstream_id"], args.kind, args.source,
        payload, args.expected_revision, args.created_at,
    )
    adapter = LinearCommentEventAdapter(
        target["client"], issue_id=target["child_workstream_id"],
        plan_revision=target["plan_revision"],
        root_issue_id=target["child_issue_id"], **target["route"],
    )
    receipt = adapter.apply(delta)
    return {
        "schema_version": 1,
        "root_workstream_id": target["root_workstream_id"],
        "child_workstream_id": target["child_workstream_id"],
        "child_issue_id": target["child_issue_id"],
        "plan_revision": target["plan_revision"],
        "generation_authority": target["generation_authority"],
        "receipt": {
            "event_id": receipt.event_id, "revision": receipt.revision,
            "remote_id": receipt.remote_id,
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
