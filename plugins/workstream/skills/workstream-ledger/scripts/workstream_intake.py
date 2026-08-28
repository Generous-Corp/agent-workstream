#!/usr/bin/env python3
"""Create or converge a reviewed Markdown plan in an explicit Linear route."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Callable

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_graph import GraphReviewRequired
from workstream_linear import (
    HttpGraphQLClient,
    LinearGraphQLTransport,
    LinearTransportError,
)
from workstream_plan import plan_payload


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("source", help="Markdown path, HTTPS URL, or - for stdin")
    value.add_argument(
        "--identity", required=True,
        help="canonical immutable identity for these exact plan bytes",
    )
    value.add_argument(
        "--plan-revision", required=True, metavar="SHA256",
        help="exact source SHA-256 from the reviewed workstreamctl plan preview",
    )
    review = value.add_mutually_exclusive_group()
    review.add_argument(
        "--accept-key", action="append", default=None, metavar="STABLE_KEY",
        help="reviewed candidate key to create; repeat for each accepted child",
    )
    review.add_argument(
        "--accept-none", action="store_true",
        help="explicitly accept a root with no child candidates",
    )
    value.add_argument("--config", help="exact .workstream.json path")
    value.add_argument("--workspace-id")
    value.add_argument("--team-id")
    value.add_argument("--project-id")
    return value


def run(
    argv: list[str],
    *,
    client_factory: Callable[[str], Any] = HttpGraphQLClient,
) -> dict[str, Any]:
    args = parser().parse_args(argv)
    if args.accept_key is None and not args.accept_none:
        raise GraphReviewRequired(
            "review candidate keys and pass --accept-key, or pass --accept-none"
        )
    if not args.identity.strip():
        raise ValueError("plan identity must be non-empty")
    accepted_keys = set(args.accept_key or [])
    plan = plan_payload(args.source, args.identity)
    if plan["root"]["plan_revision"] != args.plan_revision:
        raise GraphReviewRequired(
            "plan bytes changed after review; generate and review a new plan preview"
        )
    route, _config_path = resolve_linear_route(
        config_path=args.config,
        workspace_id=args.workspace_id,
        team_id=args.team_id,
        project_id=args.project_id,
    )
    if not route or any(
        not route.get(field) for field in ("workspace_id", "team_id", "project_id")
    ):
        raise ValueError(
            "intake requires a complete Linear workspace/team/project route via config or flags"
        )
    token = load_linear_api_key()
    if not token:
        raise ValueError(
            "Linear authentication is required via LINEAR_API_KEY or the private token file"
        )
    transport = LinearGraphQLTransport(
        client_factory(token),
        workspace_id=route["workspace_id"],
        team_id=route["team_id"],
        project_id=route["project_id"],
    )
    result = transport.intake_reviewed_plan(plan, accepted_keys=accepted_keys)
    return {
        "schema_version": 1,
        "source": plan["source"],
        "plan_revision": result["plan_revision"],
        "route": result["route"],
        "receipts": result["receipts"],
    }


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(sys.argv[1:] if argv is None else argv)
    except (
        GraphReviewRequired,
        LinearTransportError,
        OSError,
        TimeoutError,
        UnicodeDecodeError,
        ValueError,
    ) as error:
        print(f"workstream intake failed: {error}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
