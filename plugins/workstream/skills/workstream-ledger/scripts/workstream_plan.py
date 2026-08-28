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
import os
import re
import signal
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from workstream_http import default_ssl_context

SCHEMA_VERSION = 1
HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
NUMBERED = re.compile(r"^\s*(\d+)[.)]\s+(.+?)\s*$")
FENCE = re.compile(r"^\s*(```|~~~)")
GITHUB_OWNER = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?")
GITHUB_REPOSITORY = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,98}[A-Za-z0-9])?"
)
GITHUB_PATH_SEGMENT = re.compile(r"[A-Za-z0-9._+@,-]+")
EXACT_GIT_COMMIT = re.compile(r"[0-9a-f]{40}")
GIT_FETCH_TIMEOUT_SECONDS = 20
GIT_SHOW_TIMEOUT_SECONDS = 5
PROCESS_REAP_TIMEOUT_SECONDS = 2
SSH_WRAPPER = """#!/usr/bin/env python3
import os
import sys

os.execvp("ssh", ["ssh", "-oBatchMode=yes", *sys.argv[1:]])
"""


def _immutable_github_blob(source: str) -> tuple[str, str, str, str] | None:
    """Parse a strict immutable github.com blob identity for SSH fallback."""
    parsed = urlparse(source)
    if parsed.scheme != "https" or parsed.netloc.lower() != "github.com":
        return None
    if parsed.params or parsed.query or parsed.fragment:
        return None
    match = re.fullmatch(
        r"/([^/]+)/([^/]+)/blob/([^/]+)/(.+)", parsed.path,
    )
    if not match:
        return None
    owner, repository, commit, path = match.groups()
    segments = path.split("/")
    if (
        not GITHUB_OWNER.fullmatch(owner)
        or not GITHUB_REPOSITORY.fullmatch(repository)
        or not EXACT_GIT_COMMIT.fullmatch(commit)
        or len(path) > 4096
        or any(
            not segment
            or segment in {".", ".."}
            or not GITHUB_PATH_SEGMENT.fullmatch(segment)
            for segment in segments
        )
    ):
        return None
    return owner, repository, commit, path


def _git_ssh_environment() -> dict[str, str]:
    """Keep only environment needed for Git/SSH; never forward API tokens."""
    allowed = (
        "PATH", "HOME", "SSH_AUTH_SOCK", "LANG", "LC_ALL", "USER",
        "LOGNAME", "TMPDIR",
    )
    environment = {
        key: os.environ[key] for key in allowed if os.environ.get(key)
    }
    environment.update({
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
    })
    return environment


def _run_bounded(
    arguments: list[str], *, environment: dict[str, str], timeout: float,
) -> bytes:
    """Run one command in an owned process group and reap its whole tree."""
    if not hasattr(os, "killpg") or not hasattr(signal, "SIGKILL"):
        raise OSError("immutable GitHub SSH plan fetch requires process-group support")
    try:
        process = subprocess.Popen(
            arguments,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            start_new_session=True,
        )
    except OSError as error:
        raise OSError("immutable GitHub SSH plan fetch failed") from error
    try:
        stdout, _stderr = process.communicate(timeout=timeout)
    except BaseException as error:
        _kill_and_reap(process)
        if isinstance(error, subprocess.TimeoutExpired):
            raise TimeoutError("immutable GitHub SSH plan fetch timed out") from error
        raise
    if process.returncode != 0:
        _kill_and_reap(process)
        raise OSError("immutable GitHub SSH plan fetch failed")
    return stdout


def _kill_and_reap(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.communicate(timeout=PROCESS_REAP_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        try:
            process.communicate(timeout=PROCESS_REAP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as error:
            raise OSError(
                "immutable GitHub SSH plan process tree could not be reaped"
            ) from error


def _write_ssh_wrapper(directory: str) -> str:
    """Create a fixed no-shell SSH launcher that forbids interactive auth."""
    wrapper = Path(directory) / "workstream-ssh"
    wrapper.write_text(SSH_WRAPPER, encoding="utf-8")
    wrapper.chmod(0o700)
    return str(wrapper)


def _github_ssh_blob_bytes(
    owner: str, repository: str, commit: str, path: str,
) -> bytes:
    """Fetch one exact GitHub object over SSH in a disposable bare repo."""
    remote = f"git@github.com:{owner}/{repository}.git"
    with tempfile.TemporaryDirectory(prefix="workstream-plan-") as directory:
        repository_path = str(Path(directory) / "repository.git")
        environment = _git_ssh_environment()
        environment.update({
            "GIT_SSH": _write_ssh_wrapper(directory),
            "GIT_SSH_VARIANT": "ssh",
        })
        _run_bounded(
            ["git", "init", "--bare", "--quiet", repository_path],
            environment=environment, timeout=GIT_SHOW_TIMEOUT_SECONDS,
        )
        _run_bounded(
            [
                "git", "-C", repository_path, "fetch", "--quiet", "--no-tags",
                "--depth=1", remote, commit,
            ],
            environment=environment, timeout=GIT_FETCH_TIMEOUT_SECONDS,
        )
        return _run_bounded(
            [
                "git", "-C", repository_path, "show", "--no-ext-diff",
                "--no-textconv", f"{commit}:{path}",
            ],
            environment=environment, timeout=GIT_SHOW_TIMEOUT_SECONDS,
        )


def source_bytes(source: str, identity: str | None = None) -> tuple[bytes, str]:
    if source == "-":
        raw = sys.stdin.buffer.read()
        digest = hashlib.sha256(raw).hexdigest()
        return raw, identity or f"inline-sha256:{digest}"
    parsed = urlparse(source)
    if parsed.scheme in {"http", "https"}:
        fetch_url = source
        if parsed.netloc.lower() == "github.com":
            parts = parsed.path.strip("/").split("/")
            if len(parts) >= 5 and parts[2] == "blob":
                fetch_url = (
                    "https://raw.githubusercontent.com/"
                    + "/".join((parts[0], parts[1], *parts[3:]))
                )
        headers = {"User-Agent": "workstream-ledger/1"}
        github_token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if github_token and urlparse(fetch_url).netloc.lower() in {
            "github.com", "raw.githubusercontent.com", "api.github.com",
        }:
            headers["Authorization"] = f"Bearer {github_token}"
        request = Request(fetch_url, headers=headers)
        try:
            with urlopen(  # noqa: S310 - explicit user source
                request, timeout=15, context=default_ssl_context()
            ) as response:
                return response.read(), identity or source
        except HTTPError as error:
            immutable = _immutable_github_blob(source)
            if error.code != 404 or immutable is None:
                raise
            return _github_ssh_blob_bytes(*immutable), identity or source
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
