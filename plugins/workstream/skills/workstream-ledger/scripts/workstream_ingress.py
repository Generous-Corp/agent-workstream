#!/usr/bin/env python3
"""Durable, zero-model ingress for evolving agent workstream prompts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


SCHEMA_VERSION = 1
LABEL = "workstream-ingress"
MAX_PROMPT_BYTES = 16 * 1024
DEFAULT_RETENTION_DAYS = 30
DEFAULT_REMOTE_RETENTION_DAYS = 90
DEFAULT_MAX_LOCAL_BYTES = 50 * 1024 * 1024
#: Where `gh` actually lives when PATH is the one a non-interactive login shell
#: gets. A hook fires under `codex exec` and agent-spawned shells that never
#: read an interactive profile, so /opt/homebrew/bin is routinely absent and
#: the capture died with a bare FileNotFoundError.
GH_SEARCH_PATHS = ("/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/opt/local/bin")
#: File-backed token, per the agent-secrets contract: the live path an agent
#: reads is a 0600 file, never an interactive keyring, because a hook cannot
#: answer a keychain prompt.
DEFAULT_TOKEN_FILE = "~/.config/workstream/ingress-token"
#: Older rows to retry per successful capture. Bounded so the hook stays fast;
#: the point is that a backlog drains on its own instead of waiting for someone
#: to notice and run `flush`.
OPPORTUNISTIC_DRAIN = 5
CAPTURE_MARKER = "<!-- workstream-ingress:capture:v1 -->"
PROCESSED_MARKER = "<!-- workstream-ingress:processed:v1 -->"
BIND_MARKER = "<!-- workstream-ingress:bind:v1 -->"

SECRET_PATTERNS = (
    re.compile(r"-----BEGIN [^-]*(?:PRIVATE KEY|CERTIFICATE)-----.*?-----END [^-]*-----", re.S),
    re.compile(r"(?i)\b(authorization\s*:\s*bearer)\s+\S+"),
    re.compile(r"(?i)\b(token|secret|password|passwd|api[_-]?key)\s*[:=]\s*['\"]?[^\s'\"]+"),
    re.compile(r"\b(?:sk|ghp|gho|github_pat|xox[baprs])-[-A-Za-z0-9_]{16,}\b"),
)
SENSITIVE_QUERY_KEYS = re.compile(
    r"(?i)^(?:code|token|access_token|refresh_token|id_token|state|client_secret|key|password)$"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def state_root() -> Path:
    override = os.environ.get("WORKSTREAM_INGRESS_STATE_DIR")
    return Path(override) if override else Path.home() / ".local/state/workstream-ingress"


def config_path() -> Path:
    override = os.environ.get("WORKSTREAM_INGRESS_CONFIG")
    return Path(override) if override else Path.home() / ".config/workstream-ingress/config.json"


def secure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)


def classify_remote_failure(message: str) -> str:
    """Name WHY a remote call failed, so a backlog is diagnosable without a rerun.

    The three observed causes need opposite fixes and their raw messages do not
    say so. In particular GitHub's rate-limit text names an *IP address* only
    for UNAUTHENTICATED requests (60/hr per IP), so that message is really a
    missing-credential report wearing a rate-limit costume.
    """
    if "No such file or directory: 'gh'" in message or "gh not found" in message:
        return "gh-missing"
    if re.search(r"rate limit exceeded for \d+\.\d+\.\d+\.\d+", message):
        return "unauthenticated"
    if "Requires authentication" in message or "Bad credentials" in message:
        return "unauthenticated"
    if re.search(r"HTTP 5\d\d", message) or "No server is currently available" in message:
        return "github-unavailable"
    if "rate limit" in message.lower():
        return "rate-limited"
    return "other"


def gh_binary_or_none() -> str | None:
    """`gh_binary` for reporting: status must describe a broken host, not die on it."""
    try:
        return gh_binary()
    except RuntimeError:
        return None


def token_file() -> Path:
    override = os.environ.get("WORKSTREAM_INGRESS_TOKEN_FILE")
    return Path(override) if override else Path(DEFAULT_TOKEN_FILE).expanduser()


def gh_binary() -> str:
    """Resolve `gh` without trusting PATH.

    `shutil.which` first so an intentional override still wins, then the
    standard install locations. Failing loudly with the path list beats the
    bare FileNotFoundError that made this the largest single cause of
    unacknowledged rows.
    """
    override = os.environ.get("WORKSTREAM_INGRESS_GH_BIN") or load_config().get("gh_bin")
    if override and os.access(override, os.X_OK):
        return override
    found = shutil.which("gh")
    if found:
        return found
    for directory in GH_SEARCH_PATHS:
        candidate = os.path.join(directory, "gh")
        if os.access(candidate, os.X_OK):
            return candidate
    raise RuntimeError(
        "gh not found on PATH or in " + ", ".join(GH_SEARCH_PATHS)
        + " (a hook shell does not read an interactive profile; "
        "set gh_bin in the ingress config or WORKSTREAM_INGRESS_GH_BIN)"
    )


def gh_env() -> dict[str, str]:
    """Environment overlay that makes the call authenticated, or {}.

    A token already in the environment is the caller's deliberate choice and is
    left alone. Otherwise a 0600 token file is used, because `gh`'s own keyring
    store is unavailable in the non-interactive shells where this hook runs —
    and an unauthenticated request does not fail loudly, it silently spends the
    60/hr anonymous IP budget shared by every tool on the machine.
    """
    if os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN"):
        return {}
    path = token_file()
    try:
        mode = path.stat().st_mode
    except OSError:
        return {}
    if mode & 0o077:
        raise RuntimeError(f"{path} must be mode 0600; refusing to read a group/world-readable token")
    token = path.read_text().strip()
    return {"GH_TOKEN": token} if token else {}


def record_failure(stage: str, error: BaseException) -> None:
    """Record metadata-only hook failure without persisting the raw prompt."""
    try:
        path = state_root() / "failures.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(path.parent, 0o700)
        entry = {
            "at": utc_now(),
            "stage": stage,
            "error_type": type(error).__name__,
            "message": str(error)[:500],
            "cause": classify_remote_failure(str(error)),
        }
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(descriptor, "a") as stream:
            stream.write(json.dumps(entry, sort_keys=True) + "\n")
    except Exception:
        pass


def load_config() -> dict[str, Any]:
    path = config_path()
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def save_config(config: dict[str, Any]) -> None:
    path = config_path()
    secure_parent(path)
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
    os.chmod(temp, 0o600)
    temp.replace(path)


def connect() -> sqlite3.Connection:
    root = state_root()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    db = root / "outbox.sqlite3"
    conn = sqlite3.connect(db, timeout=2)
    os.chmod(db, 0o600)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=2000")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
          event_id TEXT PRIMARY KEY,
          captured_at TEXT NOT NULL,
          provider TEXT NOT NULL,
          session_id TEXT,
          turn_id TEXT,
          surface_id TEXT,
          workspace_id TEXT,
          cwd TEXT,
          workstream_id TEXT,
          context_url TEXT,
          prompt TEXT NOT NULL,
          prompt_sha256 TEXT NOT NULL,
          redactions INTEGER NOT NULL,
          truncated INTEGER NOT NULL,
          remote_repo TEXT,
          remote_issue INTEGER,
          remote_comment_id INTEGER,
          remote_url TEXT,
          remote_acked_at TEXT,
          processed_at TEXT,
          disposition TEXT,
          promoted_issue TEXT
        )
        """
    )
    # Persisted bindings. Without these, `bind` only backfilled the rows that
    # already existed, so every LATER turn of the same session was captured
    # unbound again and nothing ever revisited it — which is how 36 legacy
    # events and then 315 more accumulated behind a mechanism that looked like
    # it was working.
    #
    # `kind` is deliberately limited to identities that are trustworthy:
    # an exact provider session, or a cmux surface. There is no cwd row and no
    # heuristic row, because several tabs share one checkout and a wrong
    # binding is worse than an obvious gap — a gap is visible in `status`,
    # a wrong binding is invisible forever.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bindings (
          kind TEXT NOT NULL CHECK (kind IN ('session', 'surface')),
          identity TEXT NOT NULL,
          workstream_id TEXT NOT NULL,
          context_url TEXT,
          bound_at TEXT NOT NULL,
          PRIMARY KEY (kind, identity)
        )
        """
    )
    conn.commit()
    return conn


def redact_url(match: re.Match[str]) -> str:
    raw = match.group(0)
    try:
        parts = urlsplit(raw)
        if not parts.query:
            return raw
        pairs = []
        for piece in parts.query.split("&"):
            key, separator, value = piece.partition("=")
            if separator and SENSITIVE_QUERY_KEYS.match(key):
                pairs.append(f"{key}=[REDACTED]")
            else:
                pairs.append(piece)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "&".join(pairs), parts.fragment))
    except ValueError:
        return raw


def redact_prompt(prompt: str) -> tuple[str, int, bool]:
    redactions = 0
    value = re.sub(r"https?://[^\s<>]+", redact_url, prompt)
    for pattern in SECRET_PATTERNS:
        def replace(match: re.Match[str]) -> str:
            nonlocal redactions
            redactions += 1
            prefix = match.group(1) if match.lastindex else ""
            return f"{prefix} [REDACTED]" if prefix else "[REDACTED]"
        value = pattern.sub(replace, value)
    encoded = value.encode("utf-8")
    truncated = len(encoded) > MAX_PROMPT_BYTES
    if truncated:
        value = encoded[:MAX_PROMPT_BYTES].decode("utf-8", errors="ignore") + "\n[TRUNCATED]"
    return value, redactions, truncated


def first_string(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def prompt_from(payload: dict[str, Any]) -> str:
    direct = first_string(payload, "prompt", "text", "message", "body")
    if direct:
        return direct
    for key in ("notification", "data"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            direct = first_string(nested, "prompt", "text", "message", "body")
            if direct:
                return direct
    return ""


def event_id_for(payload: dict[str, Any], provider: str, prompt_hash: str) -> str:
    explicit = first_string(payload, "event_id", "eventId")
    session = first_string(payload, "session_id", "sessionId") or "unknown-session"
    turn = first_string(payload, "turn_id", "turnId")
    transcript = first_string(payload, "transcript_path", "transcriptPath") or ""
    cwd = first_string(payload, "cwd") or os.getcwd()
    seed = "\0".join((provider, explicit or "", session, turn or "", transcript, cwd, prompt_hash))
    return "wsi_" + hashlib.sha256(seed.encode()).hexdigest()[:32]


def event_record(payload: dict[str, Any], provider: str) -> dict[str, Any]:
    raw_prompt = prompt_from(payload)
    prompt_hash = hashlib.sha256(raw_prompt.encode()).hexdigest()
    prompt, redactions, truncated = redact_prompt(raw_prompt)
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id_for(payload, provider, prompt_hash),
        "captured_at": utc_now(),
        "provider": provider,
        "session_id": first_string(payload, "session_id", "sessionId"),
        "turn_id": first_string(payload, "turn_id", "turnId"),
        "surface_id": os.environ.get("CMUX_SURFACE_ID"),
        "workspace_id": os.environ.get("CMUX_WORKSPACE_ID"),
        "cwd": first_string(payload, "cwd") or os.getcwd(),
        "workstream_id": os.environ.get("WHENCE_WORKSTREAM_ID"),
        "context_url": os.environ.get("WORKSTREAM_CONTEXT_URL"),
        "prompt": prompt,
        "prompt_sha256": prompt_hash,
        "redactions": redactions,
        "truncated": truncated,
        "machine": socket.gethostname().split(".")[0],
    }


def resolve_binding(conn: sqlite3.Connection, event: dict[str, Any]) -> None:
    """Fill in a workstream for an event that arrived without one.

    Only exact identities are consulted, most specific first: the provider
    session, then the cmux surface. `cwd` is deliberately NOT a fallback — many
    tabs share one checkout, so binding on it would attach turns to whatever
    workstream happened to run there last. An unbound event is a visible gap;
    a wrongly bound one is a silent lie.

    An explicit WHENCE_WORKSTREAM_ID still wins, because a caller that names a
    workstream for this turn is making a more specific statement than a binding
    recorded earlier.
    """
    if event.get("workstream_id"):
        return
    for kind, identity in (("session", event.get("session_id")),
                           ("surface", event.get("surface_id"))):
        if not identity:
            continue
        row = conn.execute(
            "SELECT workstream_id, context_url FROM bindings WHERE kind=? AND identity=?",
            (kind, identity),
        ).fetchone()
        if row:
            event["workstream_id"] = row[0]
            event["context_url"] = event.get("context_url") or row[1]
            return


def record_binding(
    conn: sqlite3.Connection, kind: str, identity: str, workstream: str, context_url: str | None
) -> None:
    """Remember a binding so the session's LATER turns bind themselves."""
    conn.execute(
        "INSERT INTO bindings (kind, identity, workstream_id, context_url, bound_at) "
        "VALUES (?,?,?,?,?) ON CONFLICT(kind, identity) DO UPDATE SET "
        "workstream_id=excluded.workstream_id, context_url=excluded.context_url, "
        "bound_at=excluded.bound_at",
        (kind, identity, workstream, context_url, utc_now()),
    )


def forget_binding(conn: sqlite3.Connection, kind: str, identity: str) -> None:
    conn.execute("DELETE FROM bindings WHERE kind=? AND identity=?", (kind, identity))


def insert_event(conn: sqlite3.Connection, event: dict[str, Any]) -> None:
    conn.execute(
        """INSERT OR IGNORE INTO events
        (event_id,captured_at,provider,session_id,turn_id,surface_id,workspace_id,cwd,
         workstream_id,context_url,prompt,prompt_sha256,redactions,truncated)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            event["event_id"], event["captured_at"], event["provider"], event["session_id"],
            event["turn_id"], event["surface_id"], event["workspace_id"], event["cwd"],
            event["workstream_id"], event["context_url"], event["prompt"],
            event["prompt_sha256"], event["redactions"], int(event["truncated"]),
        ),
    )
    conn.commit()


def gh(args: list[str], *, stdin: str | None = None, timeout: float = 4) -> Any:
    env = {**os.environ, **gh_env()}
    proc = subprocess.run(
        [gh_binary(), *args], input=stdin, text=True, capture_output=True,
        timeout=timeout, check=False, env=env,
    )
    if proc.returncode:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "gh failed")
    return json.loads(proc.stdout) if proc.stdout.strip() else None


def comment_body(marker: str, payload: dict[str, Any]) -> str:
    return marker + "\n```json\n" + json.dumps(payload, sort_keys=True) + "\n```"


def upload_event(conn: sqlite3.Connection, event_id: str, config: dict[str, Any]) -> bool:
    row = conn.execute(
        "SELECT event_id,captured_at,provider,session_id,turn_id,surface_id,workspace_id,cwd,"
        "workstream_id,context_url,prompt,prompt_sha256,redactions,truncated,remote_acked_at "
        "FROM events WHERE event_id=?", (event_id,),
    ).fetchone()
    if not row or row[-1]:
        return bool(row)
    repo, issue = config.get("repo"), config.get("issue")
    if not repo or not issue:
        return False
    keys = (
        "event_id", "captured_at", "provider", "session_id", "turn_id", "surface_id",
        "workspace_id", "cwd", "workstream_id", "context_url", "prompt", "prompt_sha256",
        "redactions", "truncated",
    )
    payload = dict(zip(keys, row[:-1]))
    payload["schema_version"] = SCHEMA_VERSION
    payload["truncated"] = bool(payload["truncated"])
    response = gh(
        ["api", f"repos/{repo}/issues/{issue}/comments", "--input", "-"],
        stdin=json.dumps({"body": comment_body(CAPTURE_MARKER, payload)}),
    )
    conn.execute(
        "UPDATE events SET remote_repo=?,remote_issue=?,remote_comment_id=?,remote_url=?,remote_acked_at=? WHERE event_id=?",
        (repo, int(issue), response["id"], response["html_url"], utc_now(), event_id),
    )
    conn.commit()
    return True


def prune(conn: sqlite3.Connection, config: dict[str, Any]) -> int:
    days = int(config.get("local_retention_days", DEFAULT_RETENTION_DAYS))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat().replace("+00:00", "Z")
    cursor = conn.execute(
        "DELETE FROM events WHERE remote_acked_at IS NOT NULL AND captured_at < ?", (cutoff,)
    )
    conn.commit()
    conn.execute("PRAGMA incremental_vacuum")
    return cursor.rowcount


def enforce_bound(conn: sqlite3.Connection, config: dict[str, Any]) -> bool:
    maximum = int(config.get("max_local_bytes", DEFAULT_MAX_LOCAL_BYTES))
    logical_bytes = conn.execute("SELECT COALESCE(SUM(LENGTH(prompt)),0) FROM events").fetchone()[0]
    if logical_bytes <= maximum:
        return True
    # Only discard rows whose remote copy is acknowledged. Local-only data is
    # never silently sacrificed to satisfy a byte target.
    for (event_id, prompt_bytes) in conn.execute(
        "SELECT event_id,LENGTH(prompt) FROM events WHERE remote_acked_at IS NOT NULL "
        "ORDER BY captured_at"
    ).fetchall():
        conn.execute("DELETE FROM events WHERE event_id=?", (event_id,))
        logical_bytes -= prompt_bytes
        if logical_bytes <= maximum:
            break
    conn.commit()
    return logical_bytes <= maximum


def parse_comment(body: str, marker: str) -> dict[str, Any] | None:
    if not body.startswith(marker):
        return None
    match = re.search(r"```json\n(.*?)\n```", body, re.S)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def remote_events(repo: str, workstream: str | None = None) -> list[dict[str, Any]]:
    issues = gh([
        "issue", "list", "--repo", repo, "--state", "all", "--label", LABEL,
        "--limit", "100", "--json", "number,url,title",
    ], timeout=15)
    captures: dict[str, dict[str, Any]] = {}
    processed: set[str] = set()
    bindings: dict[str, dict[str, Any]] = {}
    for issue in issues:
        pages = gh([
            "api", "--paginate", "--slurp",
            f"repos/{repo}/issues/{issue['number']}/comments?per_page=100",
        ], timeout=20)
        comments = [comment for page in pages for comment in page] if pages and isinstance(pages[0], list) else pages
        for comment in comments:
            item = parse_comment(comment.get("body", ""), CAPTURE_MARKER)
            if item:
                item["remote_issue"] = issue["number"]
                item["remote_repo"] = repo
                item["remote_url"] = comment.get("html_url")
                captures[item["event_id"]] = item
                continue
            item = parse_comment(comment.get("body", ""), PROCESSED_MARKER)
            if item:
                processed.add(item["event_id"])
                continue
            item = parse_comment(comment.get("body", ""), BIND_MARKER)
            if item:
                bindings[item["event_id"]] = item
    for event_id, binding in bindings.items():
        if event_id in captures:
            captures[event_id]["workstream_id"] = binding.get("workstream_id")
            captures[event_id]["context_url"] = binding.get("context_url")
    result = [event for event_id, event in captures.items() if event_id not in processed]
    if workstream:
        result = [event for event in result if event.get("workstream_id") == workstream]
    return sorted(result, key=lambda event: event.get("captured_at", ""))


def ensure_remote_issue(repo: str, machine: str | None = None) -> dict[str, Any]:
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    machine = machine or socket.gethostname().split(".")[0]
    title = f"[Workstream ingress] {machine} {month}"
    subprocess.run(
        ["gh", "label", "create", LABEL, "--repo", repo, "--color", "5319E7",
         "--description", "Private rotating durable user-turn ingress", "--force"],
        text=True, capture_output=True, timeout=15, check=False,
    )
    existing = gh([
        "issue", "list", "--repo", repo, "--state", "all", "--label", LABEL,
        "--limit", "100", "--json", "number,title,url,state",
    ], timeout=15)
    exact = next((issue for issue in existing if issue["title"] == title), None)
    if exact:
        return exact
    created = gh(
        ["api", f"repos/{repo}/issues", "--input", "-"],
        stdin=json.dumps({
            "title": title,
            "labels": [LABEL],
            "body": "Private machine/month ingress. Capture, bind, and processed markers are machine-written; consumers deduplicate by event_id.",
        }),
        timeout=15,
    )
    return {
        "number": created["number"],
        "title": created["title"],
        "url": created["html_url"],
        "state": created["state"].lower(),
    }


def command_bind(args: argparse.Namespace) -> int:
    conn = connect()
    clauses = ["workstream_id IS NULL"]
    values: list[Any] = []
    event = args.event
    session = args.session
    surface = args.surface or (None if event or session else os.environ.get("CMUX_SURFACE_ID"))
    if event:
        clauses.append("event_id=?")
        values.append(event)
    elif session:
        clauses.append("session_id=?")
        values.append(session)
    elif surface:
        clauses.append("surface_id=?")
        values.append(surface)
    else:
        raise ValueError(
            "bind requires an explicit --event/--session or a trusted cmux surface; "
            "cwd-only binding is unsafe when multiple tabs share a checkout"
        )
    # Persist the identity BEFORE backfilling, so a session whose next turn
    # arrives mid-command is already covered. Backfill only ever touches rows
    # with no workstream (the clause above), so an earlier row that already
    # carries a different workstream is never rewritten.
    if event:
        row = conn.execute("SELECT session_id, surface_id FROM events WHERE event_id=?",
                           (event,)).fetchone()
        if row and row[0]:
            record_binding(conn, "session", row[0], args.workstream, args.context_url)
        elif row and row[1]:
            record_binding(conn, "surface", row[1], args.workstream, args.context_url)
    elif session:
        record_binding(conn, "session", session, args.workstream, args.context_url)
    elif surface:
        record_binding(conn, "surface", surface, args.workstream, args.context_url)

    rows = conn.execute(
        "SELECT event_id,remote_repo,remote_issue FROM events WHERE " + " AND ".join(clauses)
        + " ORDER BY captured_at DESC LIMIT ?",
        (*values, args.limit),
    ).fetchall()
    bound = []
    for event_id, remote_repo, remote_issue in rows:
        binding = {
            "schema_version": SCHEMA_VERSION,
            "event_id": event_id,
            "bound_at": utc_now(),
            "workstream_id": args.workstream,
            "context_url": args.context_url,
        }
        if remote_repo and remote_issue:
            gh(
                ["api", f"repos/{remote_repo}/issues/{remote_issue}/comments", "--input", "-"],
                stdin=json.dumps({"body": comment_body(BIND_MARKER, binding)}), timeout=10,
            )
        conn.execute(
            "UPDATE events SET workstream_id=?,context_url=? WHERE event_id=?",
            (args.workstream, args.context_url, event_id),
        )
        bound.append(event_id)
    conn.commit()
    persisted = conn.execute(
        "SELECT kind, identity FROM bindings WHERE workstream_id=?", (args.workstream,)
    ).fetchall()
    print(json.dumps({
        "workstream": args.workstream,
        "bound": bound,
        "will_auto_bind": [{"kind": kind, "identity": identity} for kind, identity in persisted],
    }, indent=2, sort_keys=True))
    return 0


def command_unbind(args: argparse.Namespace) -> int:
    """Correct a wrong binding without marking the captured turn processed."""
    conn = connect()
    clauses = ["workstream_id=?"]
    values: list[Any] = [args.workstream]
    if args.event:
        clauses.append("event_id=?")
        values.append(args.event)
    elif args.session:
        clauses.append("session_id=?")
        values.append(args.session)
    elif args.surface:
        clauses.append("surface_id=?")
        values.append(args.surface)
    else:
        raise ValueError("unbind requires an explicit --event, --session, or --surface")
    # Forget the persisted identity too. Without this, correcting a mistaken
    # binding would clear the existing rows and then silently re-apply the same
    # wrong workstream to the session's very next turn.
    if args.session:
        forget_binding(conn, "session", args.session)
    if args.surface:
        forget_binding(conn, "surface", args.surface)
    if args.event:
        row = conn.execute("SELECT session_id, surface_id FROM events WHERE event_id=?",
                           (args.event,)).fetchone()
        if row and row[0]:
            forget_binding(conn, "session", row[0])
        if row and row[1]:
            forget_binding(conn, "surface", row[1])

    rows = conn.execute(
        "SELECT event_id,remote_repo,remote_issue FROM events WHERE " + " AND ".join(clauses)
        + " ORDER BY captured_at DESC LIMIT ?",
        (*values, args.limit),
    ).fetchall()
    unbound = []
    for event_id, remote_repo, remote_issue in rows:
        binding = {
            "schema_version": SCHEMA_VERSION,
            "event_id": event_id,
            "unbound_at": utc_now(),
            "workstream_id": None,
            "context_url": None,
        }
        if remote_repo and remote_issue:
            gh(
                ["api", f"repos/{remote_repo}/issues/{remote_issue}/comments", "--input", "-"],
                stdin=json.dumps({"body": comment_body(BIND_MARKER, binding)}), timeout=10,
            )
        conn.execute(
            "UPDATE events SET workstream_id=NULL,context_url=NULL WHERE event_id=?",
            (event_id,),
        )
        unbound.append(event_id)
    conn.commit()
    print(json.dumps({"workstream": args.workstream, "unbound": unbound}, indent=2, sort_keys=True))
    return 0


def install_hook(config_file: Path, provider: str, command: str) -> bool:
    config_file.parent.mkdir(parents=True, exist_ok=True)
    data = json.loads(config_file.read_text()) if config_file.exists() else {}
    hooks = data.setdefault("hooks", {})
    entries = hooks.setdefault("UserPromptSubmit", [])
    for entry in entries:
        for hook in entry.get("hooks", []):
            if "workstream_ingress.py capture" in hook.get("command", ""):
                return False
    entries.append({
        "hooks": [{"type": "command", "command": command, "timeout": 5}]
    })
    temp = config_file.with_suffix(config_file.suffix + ".tmp")
    temp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    temp.replace(config_file)
    return True


def drain_pending(
    conn: sqlite3.Connection, config: dict[str, Any], exclude: str, limit: int
) -> int:
    """Upload up to `limit` older unacknowledged rows, oldest first.

    Stops at the first failure: if the remote just started refusing, the rest
    of the backlog will fail the same way and a hook must not spend the user's
    latency proving it.
    """
    rows = conn.execute(
        "SELECT event_id FROM events WHERE remote_acked_at IS NULL AND event_id<>? "
        "ORDER BY captured_at LIMIT ?",
        (exclude, limit),
    ).fetchall()
    drained = 0
    for (event_id,) in rows:
        try:
            drained += int(upload_event(conn, event_id, config))
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            record_failure("backlog-drain", error)
            break
    return drained


def command_capture(args: argparse.Namespace) -> int:
    try:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict):
            raise ValueError("hook payload must be an object")
        event = event_record(payload, args.provider)
        conn = connect()
        resolve_binding(conn, event)
        insert_event(conn, event)
        config = load_config()
        try:
            upload_event(conn, event["event_id"], config)
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            record_failure("remote-upload", error)
        else:
            # This turn proved the credential path works right now, so spend a
            # little of it on the backlog. Without this a transient outage or a
            # credential-less shell leaves rows unacknowledged until a human
            # notices and runs `flush` — which is how 55 rows accumulated
            # across 55 one-shot sessions before anyone looked.
            drain_pending(conn, config, event["event_id"], OPPORTUNISTIC_DRAIN)
        enforce_bound(conn, config)
    except Exception as error:
        # An observability hook must never block the user's agent prompt.
        record_failure("local-capture", error)
    print("{}")
    return 0


def command_configure(args: argparse.Namespace) -> int:
    issue = ensure_remote_issue(args.repo, args.machine)
    config = load_config()
    config.update({
        "repo": args.repo,
        "issue": issue["number"],
        "issue_url": issue["url"],
        "local_retention_days": args.local_retention_days,
        "remote_retention_days": args.remote_retention_days,
        "max_local_bytes": args.max_local_bytes,
        "machine": args.machine or socket.gethostname().split(".")[0],
    })
    save_config(config)
    print(json.dumps(config, indent=2, sort_keys=True))
    return 0


def command_install_hooks(args: argparse.Namespace) -> int:
    script = Path(__file__).resolve()
    home = Path.home()
    changed = {
        "codex": install_hook(
            home / ".codex/hooks.json", "codex",
            f"env WORKSTREAM_INGRESS_PROVIDER=codex python3 {script} capture --provider codex",
        ),
        "claude": install_hook(
            home / ".claude/settings.json", "claude",
            f"env WORKSTREAM_INGRESS_PROVIDER=claude python3 {script} capture --provider claude",
        ),
    }
    print(json.dumps({"changed": changed, "script": str(script)}, indent=2, sort_keys=True))
    return 0


def command_flush(args: argparse.Namespace) -> int:
    """Upload every unacknowledged row, stopping at the first refusal.

    Stopping is correct — if the remote just refused, the rest of the backlog
    will refuse the same way and hammering it makes things worse. Stopping
    SILENTLY is not: a flush that printed `{"pending_before": 58, "uploaded": 0}`
    and nothing else was indistinguishable from a flush with nothing to do, and
    the operator had to go read failures.jsonl separately to learn that GitHub
    was returning 503s. That is the same defect class as the hook that captured
    55 events and never said it could not upload them, so the reason is both
    recorded and reported here.
    """
    conn = connect()
    config = load_config()
    rows = conn.execute(
        "SELECT event_id FROM events WHERE remote_acked_at IS NULL ORDER BY captured_at"
    ).fetchall()
    uploaded = 0
    stopped_because: str | None = None
    stopped_detail: str | None = None
    for (event_id,) in rows:
        try:
            uploaded += int(upload_event(conn, event_id, config))
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            record_failure("flush", error)
            stopped_because = classify_remote_failure(str(error))
            stopped_detail = str(error)[:200]
            break
    summary: dict[str, Any] = {"pending_before": len(rows), "uploaded": uploaded}
    if stopped_because:
        summary["stopped_because"] = stopped_because
        summary["stopped_detail"] = stopped_detail
        summary["remaining"] = len(rows) - uploaded
    print(json.dumps(summary, sort_keys=True))
    return 0 if uploaded == len(rows) else 1


def command_recover(args: argparse.Namespace) -> int:
    repo = args.repo or load_config().get("repo")
    if not repo:
        raise ValueError("ingress repository is not configured; pass --repo or run configure")
    events = remote_events(repo, args.workstream)
    print(json.dumps({"repo": repo, "unprocessed": events}, indent=2, sort_keys=True))
    return 0


def command_process(args: argparse.Namespace) -> int:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "event_id": args.event,
        "processed_at": utc_now(),
        "disposition": args.disposition,
        "promoted_issue": args.issue,
    }
    response = gh(
        ["api", f"repos/{args.repo}/issues/{args.remote_issue}/comments", "--input", "-"],
        stdin=json.dumps({"body": comment_body(PROCESSED_MARKER, payload)}), timeout=10,
    )
    conn = connect()
    conn.execute(
        "UPDATE events SET processed_at=?,disposition=?,promoted_issue=? WHERE event_id=?",
        (payload["processed_at"], args.disposition, args.issue, args.event),
    )
    conn.commit()
    print(json.dumps({"event_id": args.event, "processed_url": response["html_url"]}, sort_keys=True))
    return 0


def command_status(args: argparse.Namespace) -> int:
    conn = connect()
    counts = conn.execute(
        "SELECT COUNT(*), SUM(remote_acked_at IS NULL), SUM(processed_at IS NULL) FROM events"
    ).fetchone()
    # Unbound volume is the number that matters and the one nobody was
    # watching: an unbound event is an open obligation no recovery pass can
    # find by workstream, and 36 of them became 315 without anything saying so.
    # Report it by session and by age, because "many sessions, days old" and
    # "one session, minutes old" call for opposite responses.
    unbound = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT session_id), MIN(captured_at) FROM events "
        "WHERE workstream_id IS NULL AND processed_at IS NULL"
    ).fetchone()
    oldest_unbound_age_hours = None
    if unbound[2]:
        try:
            oldest = datetime.fromisoformat(unbound[2].replace("Z", "+00:00"))
            delta = datetime.now(timezone.utc) - oldest
            oldest_unbound_age_hours = round(delta.total_seconds() / 3600, 1)
        except ValueError:
            oldest_unbound_age_hours = None
    binding_count = conn.execute("SELECT COUNT(*) FROM bindings").fetchone()[0]
    config = load_config()
    failure_log = state_root() / "failures.jsonl"
    failures = 0
    local_capture_failures = 0
    causes: dict[str, int] = {}
    if failure_log.exists():
        with failure_log.open(errors="replace") as stream:
            for line in stream:
                if not line.strip():
                    continue
                failures += 1
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    local_capture_failures += 1
                    continue
                local_capture_failures += int(entry.get("stage") == "local-capture")
                cause = entry.get("cause") or classify_remote_failure(entry.get("message", ""))
                causes[cause] = causes.get(cause, 0) + 1
    print(json.dumps({
        "events": counts[0] or 0,
        "not_remote_acked": counts[1] or 0,
        "not_locally_processed": counts[2] or 0,
        "database_bytes": (state_root() / "outbox.sqlite3").stat().st_size,
        "remote_repo": config.get("repo"),
        "remote_issue": config.get("issue"),
        "remote_issue_url": config.get("issue_url"),
        "capture_failures": failures,
        "unrecoverable_local_capture_failures": local_capture_failures,
        "failure_causes": causes,
        "unbound_events": unbound[0] or 0,
        "unbound_sessions": unbound[1] or 0,
        "oldest_unbound_age_hours": oldest_unbound_age_hours,
        "persisted_bindings": binding_count,
        "gh_binary": gh_binary_or_none(),
        "token_file_present": token_file().exists(),
    }, indent=2, sort_keys=True))
    return 0 if not local_capture_failures and not (counts[1] or 0) else 1


def unbound_metrics(conn: sqlite3.Connection) -> dict[str, Any]:
    """The two numbers a periodic check needs, which measure different things.

    `unbound_events` is a LEVEL. It is history: a known, tracked quantity that
    only a triage pass reduces, so alerting on it would be permanently red and
    would therefore be muted, which is worse than no check at all.

    `unbound_with_binding` is an INVARIANT. Capture resolves a workstream from
    the bindings table, so an event whose session or surface is already bound
    can never legitimately be unbound. Any nonzero value means capture-time
    resolution regressed — this is the number that means something broke now.
    """
    total, sessions = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT session_id) FROM events "
        "WHERE workstream_id IS NULL AND processed_at IS NULL"
    ).fetchone()
    violations = conn.execute(
        "SELECT COUNT(*) FROM events e WHERE e.workstream_id IS NULL "
        "AND e.processed_at IS NULL AND EXISTS ("
        "  SELECT 1 FROM bindings b WHERE"
        "    (b.kind='session' AND b.identity=e.session_id) OR"
        "    (b.kind='surface' AND b.identity=e.surface_id))"
    ).fetchone()[0]
    return {
        "unbound_events": total or 0,
        "unbound_sessions": sessions or 0,
        "unbound_with_binding": violations,
    }


def command_ratchet(args: argparse.Namespace) -> int:
    """Flag GROWTH in the unbound backlog, not its level.

    The baseline is rewritten on every run, including a run that grew. That is
    deliberate: a ratchet that held the old baseline after an increase would
    stay red until the whole backlog was triaged, and a permanently red check
    gets muted. Rewriting means each interval is judged on its own, so the
    alert fires the moment resolution regresses and clears once it stops.
    """
    conn = connect()
    current = unbound_metrics(conn)
    path = state_root() / "ratchet.json"
    previous: dict[str, Any] = {}
    if path.exists():
        try:
            previous = json.loads(path.read_text())
        except json.JSONDecodeError:
            previous = {}

    baseline = previous.get("unbound_events")
    grew_by = 0 if baseline is None else current["unbound_events"] - baseline
    report = {
        **current,
        "previous_unbound_events": baseline,
        "previous_observed_at": previous.get("observed_at"),
        "grew_by": grew_by,
        "first_observation": baseline is None,
    }

    secure_parent(path)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump({**current, "observed_at": utc_now()}, stream, sort_keys=True)

    alerts = []
    if current["unbound_with_binding"]:
        alerts.append(
            f"{current['unbound_with_binding']} event(s) are unbound despite their "
            "session or surface already being bound: capture-time resolution regressed"
        )
    if grew_by > 0:
        alerts.append(
            f"the unbound backlog grew by {grew_by} since "
            f"{previous.get('observed_at', 'the last check')}"
        )
    report["alerts"] = alerts
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if alerts else 0


def command_prune(args: argparse.Namespace) -> int:
    conn = connect()
    deleted = prune(conn, load_config())
    conn.execute("VACUUM")
    print(json.dumps({"pruned": deleted, "database_bytes": (state_root() / "outbox.sqlite3").stat().st_size}))
    return 0


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    capture = sub.add_parser("capture")
    capture.add_argument("--provider", choices=("codex", "claude"), required=True)
    capture.set_defaults(func=command_capture)
    configure = sub.add_parser("configure")
    configure.add_argument("--repo", required=True)
    configure.add_argument("--machine")
    configure.add_argument("--local-retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    configure.add_argument("--remote-retention-days", type=int, default=DEFAULT_REMOTE_RETENTION_DAYS)
    configure.add_argument("--max-local-bytes", type=int, default=DEFAULT_MAX_LOCAL_BYTES)
    configure.set_defaults(func=command_configure)
    bind = sub.add_parser("bind")
    bind.add_argument("--workstream", required=True)
    bind.add_argument("--context-url", required=True)
    bind_identity = bind.add_mutually_exclusive_group()
    bind_identity.add_argument("--event")
    bind_identity.add_argument("--session")
    bind_identity.add_argument("--surface")
    bind.add_argument("--limit", type=int, default=100)
    bind.set_defaults(func=command_bind)
    unbind = sub.add_parser("unbind")
    unbind.add_argument("--workstream", required=True)
    unbind_identity = unbind.add_mutually_exclusive_group(required=True)
    unbind_identity.add_argument("--event")
    unbind_identity.add_argument("--session")
    unbind_identity.add_argument("--surface")
    unbind.add_argument("--limit", type=int, default=100)
    unbind.set_defaults(func=command_unbind)
    install = sub.add_parser("install-hooks")
    install.set_defaults(func=command_install_hooks)
    flush = sub.add_parser("flush")
    flush.set_defaults(func=command_flush)
    recover = sub.add_parser("recover")
    recover.add_argument("--repo")
    recover.add_argument("--workstream")
    recover.set_defaults(func=command_recover)
    process = sub.add_parser("process")
    process.add_argument("--repo", required=True)
    process.add_argument("--remote-issue", type=int, required=True)
    process.add_argument("--event", required=True)
    process.add_argument("--disposition", choices=("promoted", "no-material-delta", "superseded"), required=True)
    process.add_argument("--issue")
    process.set_defaults(func=command_process)
    status = sub.add_parser("status")
    status.set_defaults(func=command_status)
    sub.add_parser("ratchet").set_defaults(func=command_ratchet)

    prune_parser = sub.add_parser("prune")
    prune_parser.set_defaults(func=command_prune)
    return p


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
