#!/usr/bin/env python3
"""Deterministically turn a Markdown plan into a Linear-ready work graph.

This command is deliberately model-free and side-effect free: it snapshots the
exact source revision, extracts actionable sections, and emits a stable JSON
payload that the Linear adapter (or an agent using Linear tools) can upsert.
It never guesses a repository, worktree, or mutable issue status.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from workstream_http import default_ssl_context

SCHEMA_VERSION = 1
HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
NUMBERED = re.compile(r"^\s*(\d+)[.)]\s+(.+?)\s*$")
FENCE = re.compile(r"^\s*(```|~~~)")


def source_bytes(source: str, identity: str | None = None) -> tuple[bytes, str]:
    if source == "-":
        raw = sys.stdin.buffer.read()
        digest = hashlib.sha256(raw).hexdigest()
        return raw, identity or f"inline-sha256:{digest}"
    parsed = urlparse(source)
    if parsed.scheme in {"http", "https"}:
        request = Request(source, headers={"User-Agent": "workstream-ledger/1"})
        with urlopen(  # noqa: S310 - explicit user source
            request, timeout=15, context=default_ssl_context()
        ) as response:
            return response.read(), identity or source
    path = Path(source).expanduser().resolve()
    return path.read_bytes(), identity or str(path)


def clean_title(text: str) -> str:
    title = text.lstrip("#").strip()
    title = re.sub(r"^(?:plan|project)\s*:\s*", "", title, flags=re.I)
    return title or "Untitled workstream"


def first_heading(markdown: str) -> re.Match[str] | None:
    fence_marker: str | None = None
    for line in markdown.splitlines():
        fence = FENCE.match(line)
        if fence:
            marker = fence.group(1)
            if fence_marker is None:
                fence_marker = marker
            elif marker == fence_marker:
                fence_marker = None
            continue
        if fence_marker is None and (heading := HEADING.match(line)):
            return heading
    return None


def stable_key(kind: str, ancestry: list[str], title: str, occurrence: int) -> str:
    """Return an identity stable across unrelated edits to the same plan."""
    material = json.dumps(
        [kind, *ancestry, clean_title(title).casefold(), occurrence],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"{kind}-{hashlib.sha256(material).hexdigest()[:16]}"


def extract_children(markdown: str) -> list[dict[str, object]]:
    lines = markdown.splitlines()
    children: list[dict[str, object]] = []
    heading_path: list[str] = []
    occurrences: dict[tuple[str, ...], int] = {}
    fence_marker: str | None = None
    for index, line in enumerate(lines, 1):
        fence = FENCE.match(line)
        if fence:
            marker = fence.group(1)
            if fence_marker is None:
                fence_marker = marker
            elif marker == fence_marker:
                fence_marker = None
            continue
        if fence_marker is not None:
            continue
        heading = HEADING.match(line)
        if heading:
            level = len(heading.group(1))
            title = clean_title(heading.group(2))
            heading_path = heading_path[: level - 1]
            heading_path.append(title.casefold())
            if level >= 2:
                occurrence_key = ("section", *heading_path)
                occurrence = occurrences.get(occurrence_key, 0) + 1
                occurrences[occurrence_key] = occurrence
                children.append(
                    {
                        "key": stable_key("section", heading_path[:-1], title, occurrence),
                        "kind": "section",
                        "title": title,
                        "line": index,
                    }
                )
            continue
        numbered = NUMBERED.match(line)
        if numbered:
            title = numbered.group(2)
            occurrence_key = ("item", *heading_path, clean_title(title).casefold())
            occurrence = occurrences.get(occurrence_key, 0) + 1
            occurrences[occurrence_key] = occurrence
            children.append(
                {
                    "key": stable_key("item", heading_path, title, occurrence),
                    "kind": "numbered_item",
                    "title": title,
                    "line": index,
                }
            )
    return children


def plan_payload(source: str, identity: str | None = None) -> dict[str, object]:
    raw, identity = source_bytes(source, identity)
    text = raw.decode("utf-8")
    title_heading = first_heading(text)
    title = clean_title(title_heading.group(2)) if title_heading else "Untitled workstream"
    digest = hashlib.sha256(raw).hexdigest()
    identity_digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return {
        "schema_version": SCHEMA_VERSION,
        "source": {"identity": identity, "sha256": digest, "bytes": len(raw)},
        "root": {"stable_key": f"source-{identity_digest[:16]}", "title": title, "plan_revision": digest},
        "children": extract_children(text),
        "graph_review_required": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="Markdown path, HTTPS URL, or - for exact bytes on stdin")
    parser.add_argument(
        "--identity",
        help="canonical durable plan identity (recommended for a checkout path or stdin)",
    )
    args = parser.parse_args()
    try:
        json.dump(plan_payload(args.source, args.identity), sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    except (OSError, UnicodeDecodeError, TimeoutError, ValueError) as error:
        print(f"workstream plan intake failed: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
