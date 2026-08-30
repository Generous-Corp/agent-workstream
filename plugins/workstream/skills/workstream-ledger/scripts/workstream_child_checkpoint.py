#!/usr/bin/env python3
"""Persist one material-fenced checkpoint on an exact workstream child."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Callable

from workstream_child_target import add_target_arguments, authenticate_child_target
from workstream_checkpoint import CheckpointError, validate_checkpoint
from workstream_linear import HttpGraphQLClient, LinearTransportError
from workstream_linear_checkpoints import LinearCheckpointAdapter


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    add_target_arguments(value)
    value.add_argument("--checkpoint", required=True, metavar="JSON")
    value.add_argument("--material-revision", required=True, type=int)
    predecessor = value.add_mutually_exclusive_group(required=True)
    predecessor.add_argument("--predecessor-event-id")
    predecessor.add_argument("--no-predecessor", action="store_true")
    return value


def run(
    argv: list[str], *,
    client_factory: Callable[[str], Any] = HttpGraphQLClient,
) -> dict[str, Any]:
    args = parser().parse_args(argv)
    if args.material_revision < 0:
        raise ValueError("child material revision must be non-negative")
    try:
        checkpoint = json.loads(Path(args.checkpoint).read_text())
    except json.JSONDecodeError as error:
        raise ValueError("checkpoint file must contain valid JSON") from error
    if not isinstance(checkpoint, dict):
        raise ValueError("checkpoint file must contain a JSON object")
    validate_checkpoint(checkpoint)
    expected_predecessor = (
        None if args.no_predecessor else args.predecessor_event_id
    )
    if checkpoint.get("root_revision") != args.material_revision:
        raise ValueError("checkpoint material revision fence mismatch")
    if checkpoint.get("predecessor_event_id") != expected_predecessor:
        raise ValueError("checkpoint predecessor fence mismatch")
    target = authenticate_child_target(args, client_factory=client_factory)
    if (
        checkpoint.get("workstream_id") != target["child_workstream_id"]
        or checkpoint.get("plan_revision") != target["plan_revision"]
    ):
        raise ValueError("checkpoint child or plan identity mismatch")
    adapter = LinearCheckpointAdapter(
        target["client"], issue_id=target["child_workstream_id"],
        workstream_id=target["child_workstream_id"],
        issue_uuid=target["child_issue_id"], **target["route"],
    )
    receipt = adapter.persist(checkpoint)
    return {
        "schema_version": 1,
        "root_workstream_id": target["root_workstream_id"],
        "child_workstream_id": target["child_workstream_id"],
        "child_issue_id": target["child_issue_id"],
        "plan_revision": target["plan_revision"],
        "generation_authority": target["generation_authority"],
        "receipt": receipt,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(sys.argv[1:] if argv is None else argv)
    except (
        CheckpointError, LinearTransportError, OSError, TimeoutError, ValueError,
    ) as error:
        print(f"workstream child checkpoint failed: {error}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
