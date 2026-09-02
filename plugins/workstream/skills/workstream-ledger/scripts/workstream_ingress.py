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
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from workstream_delta import Delta, MutationReceipt, RevisionConflict
from workstream_linear_events import (
    LinearCommentEventAdapter, encode_event_comment, reduce_event_comments,
)


SCHEMA_VERSION = 1
LABEL = "workstream-ingress"
MAX_PROMPT_BYTES = 16 * 1024
DEFAULT_RETENTION_DAYS = 30
DEFAULT_REMOTE_RETENTION_DAYS = 90
DEFAULT_MAX_LOCAL_BYTES = 50 * 1024 * 1024
#: Common `gh` locations used when a non-interactive capture launcher has a
#: minimal PATH. An explicit configured path still wins.
GH_SEARCH_PATHS = ("/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/opt/local/bin")
#: Optional file-backed token for non-interactive capture launchers.
DEFAULT_TOKEN_FILE = "~/.config/workstream/ingress-token"
#: Older rows to retry per successful capture. Bounded so capture stays fast;
#: the point is that a backlog drains on its own instead of waiting for someone
#: to notice and run `flush`.
OPPORTUNISTIC_DRAIN = 5
CAPTURE_MARKER = "<!-- workstream-ingress:capture:v1 -->"
PROCESSED_MARKER = "<!-- workstream-ingress:processed:v1 -->"
BIND_MARKER = "<!-- workstream-ingress:bind:v1 -->"
PROMOTION_MARKER = "<!-- workstream-ingress:promotion:v2 -->"
PROMOTION_SCHEMA_VERSION = 2
MAX_PROMOTION_CHANGES = 32
MAX_PROMOTION_BYTES = 16 * 1024
MAX_PROMOTION_CONFLICTS = 8
MAX_CLASSIFICATION_HINT_GROUPS = 256
MAX_CLASSIFICATION_HINTS_REPORTED = 8

SECRET_PATTERNS = (
    re.compile(r"-----BEGIN [^-]*(?:PRIVATE KEY|CERTIFICATE)-----.*?-----END [^-]*-----", re.S),
    re.compile(r"(?i)\b(authorization\s*:\s*bearer)\s+\S+"),
    re.compile(r"(?i)\b(token|secret|password|passwd|api[_-]?key)\s*[:=]\s*['\"]?[^\s'\"]+"),
    re.compile(r"\b(?:sk[-_]|ghp[-_]|gho[-_]|github_pat[-_]|xox[baprs][-_])[-A-Za-z0-9_]{16,}\b"),
)
SENSITIVE_QUERY_KEYS = re.compile(
    r"(?i)^(?:code|token|access_token|refresh_token|id_token|state|client_secret|key|password)$"
)


class IngressConnection(sqlite3.Connection):
    """Close a discarded outbox handle before Python reports a resource leak."""

    def __del__(self) -> None:
        try:
            self.close()
        except sqlite3.Error:
            pass


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
        + " (a non-interactive launcher may not read an interactive profile; "
        "set gh_bin in the ingress config or WORKSTREAM_INGRESS_GH_BIN)"
    )


def gh_env() -> dict[str, str]:
    """Environment overlay that makes the call authenticated, or {}.

    A token already in the environment is the caller's deliberate choice and is
    left alone. Otherwise a 0600 token file is used, because `gh`'s own keyring
    may be unavailable in a non-interactive capture launcher —
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
    """Record sanitized failure metadata without persisting raw exception text."""
    try:
        path = state_root() / "failures.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(path.parent, 0o700)
        raw_message = str(error)
        safe_message, _, _ = redact_text(raw_message, max_bytes=500)
        entry = {
            "at": utc_now(),
            "stage": stage,
            "error_type": type(error).__name__,
            "message": safe_message,
            "cause": classify_remote_failure(raw_message),
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
    conn = sqlite3.connect(db, timeout=2, factory=IngressConnection)
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
    # Persisted bindings ensure later turns resolve after a session is bound.
    #
    # `kind` is deliberately limited to identities that are trustworthy:
    # an exact provider session or an adapter-provided surface. There is no cwd row and no
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
        netloc = parts.netloc
        if "@" in netloc:
            netloc = "[REDACTED]@" + netloc.rsplit("@", 1)[1]
        pairs = []
        for piece in parts.query.split("&"):
            key, separator, value = piece.partition("=")
            if separator and SENSITIVE_QUERY_KEYS.match(key):
                pairs.append(f"{key}=[REDACTED]")
            else:
                pairs.append(piece)
        return urlunsplit((parts.scheme, netloc, parts.path, "&".join(pairs), parts.fragment))
    except ValueError:
        return raw


def redact_text(text: str, *, max_bytes: int) -> tuple[str, int, bool]:
    redactions = 0
    def replace_url(match: re.Match[str]) -> str:
        nonlocal redactions
        value = redact_url(match)
        redactions += int(value != match.group(0))
        return value

    value = re.sub(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s<>]+", replace_url, text)
    for pattern in SECRET_PATTERNS:
        def replace(match: re.Match[str]) -> str:
            nonlocal redactions
            redactions += 1
            prefix = match.group(1) if match.lastindex else ""
            return f"{prefix} [REDACTED]" if prefix else "[REDACTED]"
        value = pattern.sub(replace, value)
    encoded = value.encode("utf-8")
    truncated = len(encoded) > max_bytes
    if truncated:
        value = encoded[:max_bytes].decode("utf-8", errors="ignore") + "\n[TRUNCATED]"
    return value, redactions, truncated


def redact_prompt(prompt: str) -> tuple[str, int, bool]:
    return redact_text(prompt, max_bytes=MAX_PROMPT_BYTES)


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
        "surface_id": os.environ.get("WORKSTREAM_SURFACE_ID") or os.environ.get("CMUX_SURFACE_ID"),
        "workspace_id": os.environ.get("WORKSTREAM_WORKSPACE_ID") or os.environ.get("CMUX_WORKSPACE_ID"),
        "cwd": first_string(payload, "cwd") or os.getcwd(),
        "workstream_id": os.environ.get("WORKSTREAM_ID") or os.environ.get("WHENCE_WORKSTREAM_ID"),
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
    session, then an adapter-provided surface. `cwd` is deliberately NOT a fallback — many
    tabs share one checkout, so binding on it would attach turns to whatever
    workstream happened to run there last. An unbound event is a visible gap;
    a wrongly bound one is a silent lie.

    An explicit `WORKSTREAM_ID` still wins. The legacy `WHENCE_WORKSTREAM_ID`
    is accepted only as an optional adapter input; either explicit value is
    more specific than a binding recorded earlier.
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


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonical_ingress_route(repo: Any, issue: Any) -> dict[str, Any]:
    if (
        not isinstance(repo, str)
        or not re.fullmatch(
            r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})/[A-Za-z0-9._-]{1,100}", repo
        )
        or repo.endswith(".git")
    ):
        raise ValueError("ingress_repository_route_invalid")
    if not isinstance(issue, int) or isinstance(issue, bool) or issue <= 0:
        raise ValueError("ingress_issue_route_invalid")
    return {"provider": "github", "repository": repo.lower(), "issue": issue}


def validate_output_envelope(marker: str, payload: dict[str, Any]) -> None:
    size = len(comment_body(marker, payload).encode("utf-8"))
    if size > MAX_PROMOTION_BYTES:
        raise ValueError(f"ingress_output_envelope_over_budget:{size}>{MAX_PROMOTION_BYTES}")


def promotion_id_for(value: dict[str, Any]) -> str:
    material = canonical_json(value).encode("utf-8")
    return "wsp_" + hashlib.sha256(material).hexdigest()[:32]


def load_promotion_request(path: str) -> dict[str, Any]:
    raw = sys.stdin.read() if path == "-" else Path(path).read_text()
    if len(raw.encode("utf-8")) > MAX_PROMOTION_BYTES:
        raise ValueError("promotion_request_over_budget")
    try:
        request = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError("promotion_request_invalid_json") from error
    if not isinstance(request, dict) or set(request) != {
        "schema_version", "ingress", "authority", "workstream_id",
        "plan_revision", "expected_material_revision", "changes",
    }:
        raise ValueError("promotion_request_schema_invalid")
    if type(request["schema_version"]) is not int or request["schema_version"] != 1:
        raise ValueError("promotion_request_schema_unsupported")
    ingress = request["ingress"]
    if not isinstance(ingress, dict) or set(ingress) != {
        "repo", "remote_issue", "event_id", "prompt_sha256",
    }:
        raise ValueError("promotion_request_ingress_invalid")
    for field in ("repo", "event_id"):
        if not isinstance(ingress[field], str) or not ingress[field]:
            raise ValueError(f"promotion_request_{field}_invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", ingress["prompt_sha256"] or ""):
        raise ValueError("promotion_request_prompt_sha256_invalid")
    if (
        not isinstance(ingress["remote_issue"], int)
        or isinstance(ingress["remote_issue"], bool)
        or ingress["remote_issue"] <= 0
    ):
        raise ValueError("promotion_request_remote_issue_invalid")
    canonical_ingress_route(ingress["repo"], ingress["remote_issue"])
    if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", request["workstream_id"] or ""):
        raise ValueError("promotion_request_workstream_invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", request["plan_revision"] or ""):
        raise ValueError("promotion_request_plan_revision_invalid")
    authority = request["authority"]
    required_authority = {"workspace_id", "team_id", "project_id", "root_issue_id"}
    if not isinstance(authority, dict) or set(authority) != required_authority:
        raise ValueError("promotion_request_authority_invalid")
    for field in sorted(required_authority):
        value = authority[field]
        try:
            valid = isinstance(value, str) and str(uuid.UUID(value)) == value.lower()
        except ValueError:
            valid = False
        if not valid:
            raise ValueError(f"promotion_request_{field}_invalid")
    revision = request["expected_material_revision"]
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
        raise ValueError("promotion_request_expected_revision_invalid")
    changes = request["changes"]
    if not isinstance(changes, list) or not changes or len(changes) > MAX_PROMOTION_CHANGES:
        raise ValueError("promotion_request_changes_invalid")
    for change in changes:
        if (
            not isinstance(change, dict)
            or set(change) != {"kind", "payload"}
            or not isinstance(change["kind"], str)
            or not change["kind"]
            or not isinstance(change["payload"], dict)
        ):
            raise ValueError("promotion_request_change_invalid")
    return request


def promotion_payload(request: dict[str, Any], capture: dict[str, Any]) -> dict[str, Any]:
    immutable = {
        "schema_version": PROMOTION_SCHEMA_VERSION,
        "event_id": request["ingress"]["event_id"],
        "prompt_sha256": request["ingress"]["prompt_sha256"],
        "workstream_id": request["workstream_id"],
        "plan_revision": request["plan_revision"],
        "authority": request["authority"],
        "ingress_route": canonical_ingress_route(
            request["ingress"]["repo"], request["ingress"]["remote_issue"]
        ),
        "expected_material_revision": request["expected_material_revision"],
        "changes": request["changes"],
        # A source timestamp is stable across machines and retries. It is the
        # material boundary's causal time, not a claim about mutation time.
        "source_captured_at": capture["captured_at"],
    }
    return {**immutable, "promotion_id": promotion_id_for(immutable)}


def validate_promotion_payload(promotion: Any) -> dict[str, Any]:
    if not isinstance(promotion, dict) or set(promotion) != {
        "schema_version", "event_id", "prompt_sha256", "workstream_id",
        "plan_revision", "authority", "ingress_route", "expected_material_revision", "changes",
        "source_captured_at", "promotion_id",
    }:
        raise ValueError("promotion_marker_schema_invalid")
    request = {
        "schema_version": promotion["schema_version"],
        "ingress": {
            # These route fields are not repeated in the marker; fixed safe
            # placeholders let the shared type/budget validator cover every
            # material field without inventing route identity.
            "repo": "validated/marker",
            "remote_issue": 1,
            "event_id": promotion["event_id"],
            "prompt_sha256": promotion["prompt_sha256"],
        },
        "workstream_id": promotion["workstream_id"],
        "plan_revision": promotion["plan_revision"],
        "authority": promotion["authority"],
        "expected_material_revision": promotion["expected_material_revision"],
        "changes": promotion["changes"],
    }
    # Validate the same shapes as a first-attempt request without reading a
    # file or trusting a successor's local state.
    if (
        type(request["schema_version"]) is not int
        or request["schema_version"] != PROMOTION_SCHEMA_VERSION
    ):
        raise ValueError("promotion_marker_schema_unsupported")
    if not isinstance(promotion["event_id"], str) or not promotion["event_id"]:
        raise ValueError("promotion_marker_event_id_invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", promotion["prompt_sha256"] or ""):
        raise ValueError("promotion_marker_prompt_sha256_invalid")
    if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", promotion["workstream_id"] or ""):
        raise ValueError("promotion_marker_workstream_invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", promotion["plan_revision"] or ""):
        raise ValueError("promotion_marker_plan_revision_invalid")
    authority = promotion["authority"]
    required_authority = {"workspace_id", "team_id", "project_id", "root_issue_id"}
    if not isinstance(authority, dict) or set(authority) != required_authority:
        raise ValueError("promotion_marker_authority_invalid")
    for field in sorted(required_authority):
        value = authority[field]
        try:
            valid = isinstance(value, str) and str(uuid.UUID(value)) == value.lower()
        except ValueError:
            valid = False
        if not valid:
            raise ValueError(f"promotion_marker_{field}_invalid")
    route = promotion["ingress_route"]
    if not isinstance(route, dict) or set(route) != {"provider", "repository", "issue"}:
        raise ValueError("promotion_marker_ingress_route_invalid")
    canonical_route = canonical_ingress_route(route.get("repository"), route.get("issue"))
    if route != canonical_route:
        raise ValueError("promotion_marker_ingress_route_not_canonical")
    revision = promotion["expected_material_revision"]
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
        raise ValueError("promotion_marker_expected_revision_invalid")
    changes = promotion["changes"]
    if not isinstance(changes, list) or not changes or len(changes) > MAX_PROMOTION_CHANGES:
        raise ValueError("promotion_marker_changes_invalid")
    for change in changes:
        if (
            not isinstance(change, dict) or set(change) != {"kind", "payload"}
            or not isinstance(change["kind"], str) or not change["kind"]
            or not isinstance(change["payload"], dict)
        ):
            raise ValueError("promotion_marker_change_invalid")
    if not isinstance(promotion["source_captured_at"], str) or not promotion["source_captured_at"]:
        raise ValueError("promotion_marker_source_time_invalid")
    immutable = {key: value for key, value in promotion.items() if key != "promotion_id"}
    if promotion.get("promotion_id") != promotion_id_for(immutable):
        raise ValueError(f"promotion_digest_mismatch:{promotion['event_id']}")
    validate_output_envelope(PROMOTION_MARKER, promotion)
    return promotion


def promotion_delta(promotion: dict[str, Any]) -> Delta:
    return Delta(
        event_id="wsd_" + hashlib.sha256(
            ("workstream-ingress-promotion-v1\0" + promotion["promotion_id"]).encode("utf-8")
        ).hexdigest()[:32],
        workstream_id=promotion["workstream_id"],
        kind="material_boundary",
        source="user_turn",
        payload={
            "boundary_id": "ingress:" + promotion["event_id"],
            "changes": promotion["changes"],
            "ingress": {
                "event_id": promotion["event_id"],
                "prompt_sha256": promotion["prompt_sha256"],
                "promotion_id": promotion["promotion_id"],
                "plan_revision": promotion["plan_revision"],
                "route": promotion["ingress_route"],
            },
        },
        expected_revision=promotion["expected_material_revision"],
        created_at=promotion["source_captured_at"],
    )


def apply_promotion_delta(
    adapter: LinearCommentEventAdapter, delta: Delta, *, max_conflicts: int = MAX_PROMOTION_CONFLICTS
) -> MutationReceipt:
    current = delta
    for conflict in range(max_conflicts + 1):
        try:
            return adapter.apply(current)
        except RevisionConflict:
            if conflict >= max_conflicts:
                raise
            live_revision = adapter.current_revision(current.workstream_id)
            if live_revision <= current.expected_revision:
                raise RevisionConflict("conflict did not expose a newer live revision")
            current = Delta(
                current.event_id, current.workstream_id, current.kind, current.source,
                current.payload, live_revision, current.created_at,
            )
    raise AssertionError("unreachable")


def verify_processed_promotion(
    adapter: LinearCommentEventAdapter, delta: Delta, processed: dict[str, Any]
) -> MutationReceipt:
    """Read-only proof that a processed marker names the exact Linear event."""
    state = reduce_event_comments(adapter.comments(), workstream_id=delta.workstream_id)
    existing = next((item for item in state.events if item.event_id == delta.event_id), None)
    if existing is None:
        raise ValueError("processed_material_event_missing")
    if not (
        existing.workstream_id == delta.workstream_id
        and existing.kind == delta.kind
        and existing.source == delta.source
        and existing.payload == delta.payload
        and existing.created_at == delta.created_at
        and existing.expected_revision >= delta.expected_revision
    ):
        raise ValueError("processed_material_event_mismatch")
    revision = next(
        index for index, item in enumerate(state.events, start=1)
        if item.event_id == delta.event_id
    )
    remote_id = state.remote_ids[delta.event_id]
    if (
        processed.get("material_revision") != revision
        or processed.get("material_remote_id") != remote_id
    ):
        raise ValueError("processed_material_receipt_mismatch")
    return MutationReceipt(delta.event_id, revision, remote_id)


def linear_adapter_for_promotion(
    promotion: dict[str, Any], *, config_path: str | None = None,
) -> LinearCommentEventAdapter:
    from workstream_config import load_linear_api_key, resolve_linear_route
    from workstream_linear import HttpGraphQLClient

    token = load_linear_api_key()
    if not token:
        raise ValueError("linear_auth_unavailable")
    configured, _resolved = resolve_linear_route(config_path=config_path)
    configured = configured or {}
    authority = promotion["authority"]
    for field in ("workspace_id", "team_id", "project_id"):
        if configured.get(field) and configured[field] != authority[field]:
            raise ValueError(f"promotion_config_{field}_mismatch")
    return LinearCommentEventAdapter(
        HttpGraphQLClient(token), issue_id=promotion["workstream_id"],
        workspace_id=authority["workspace_id"], team_id=authority["team_id"],
        project_id=authority["project_id"], root_issue_id=authority["root_issue_id"],
        plan_revision=promotion["plan_revision"],
    )


def promotion_failpoint(_stage: str) -> None:
    """Injectable crash point. Production deliberately performs no action."""


def _payload_without_time(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "processed_at"}


def _is_exact_legacy_processed_hint(payload: Any) -> bool:
    """Recognize the original five-field marker without granting it authority."""
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version", "event_id", "processed_at", "disposition", "promoted_issue",
    }:
        return False
    promoted_issue = payload.get("promoted_issue")
    return (
        type(payload.get("schema_version")) is int
        and payload["schema_version"] == 1
        and isinstance(payload.get("event_id"), str)
        and bool(payload["event_id"])
        and len(payload["event_id"].encode("utf-8")) <= 256
        and isinstance(payload.get("processed_at"), str)
        and bool(payload["processed_at"])
        and payload.get("disposition")
        in {"promoted", "no-material-delta", "superseded"}
        and (
            promoted_issue is None
            or (
                isinstance(promoted_issue, str)
                and bool(promoted_issue)
                and len(promoted_issue.encode("utf-8")) <= 256
            )
        )
    )


def reduce_ingress_comments(
    comments: list[dict[str, Any]], *, event_id: str,
    repo: str | None = None, issue: int | None = None,
) -> dict[str, Any]:
    """Reduce one raw event and its durable successor chain without guessing."""
    captures: list[dict[str, Any]] = []
    promotions: list[dict[str, Any]] = []
    processed: list[dict[str, Any]] = []
    bindings: list[dict[str, Any]] = []
    for comment in comments:
        body = comment.get("body", "")
        if not isinstance(body, str):
            raise ValueError("ingress_comment_body_invalid")
        for marker, target in (
            (CAPTURE_MARKER, captures), (PROMOTION_MARKER, promotions),
            (PROCESSED_MARKER, processed), (BIND_MARKER, bindings),
        ):
            item = parse_comment(body, marker)
            if item and item.get("event_id") == event_id:
                target.append(item)
                break

    def one_logical(items: list[dict[str, Any]], name: str, *, ignore_time: bool = False):
        if not items:
            return None
        canonical = {
            canonical_json(_payload_without_time(item) if ignore_time else item) for item in items
        }
        if len(canonical) != 1:
            raise ValueError(f"conflicting_{name}:{event_id}")
        return items[0]

    capture = one_logical(captures, "capture")
    promotion = one_logical(promotions, "promotion")
    disposition = one_logical(processed, "processed", ignore_time=True)
    binding = bindings[-1] if bindings else None
    if capture and binding:
        capture = {**capture, "workstream_id": binding.get("workstream_id"),
                   "context_url": binding.get("context_url")}
    if promotion:
        validate_promotion_payload(promotion)
        if repo is not None or issue is not None:
            if promotion["ingress_route"] != canonical_ingress_route(repo, issue):
                raise ValueError(f"promotion_ingress_route_mismatch:{event_id}")
        if not capture:
            raise ValueError(f"promotion_without_capture:{event_id}")
        if (
            promotion.get("prompt_sha256") != capture.get("prompt_sha256")
            or promotion.get("workstream_id") != capture.get("workstream_id")
            or promotion.get("source_captured_at") != capture.get("captured_at")
        ):
            raise ValueError(f"promotion_capture_mismatch:{event_id}")
    legacy_disposition = _is_exact_legacy_processed_hint(disposition)
    if disposition and not legacy_disposition:
        if disposition.get("disposition") == "promoted":
            if set(disposition) != {
                "schema_version", "event_id", "processed_at", "disposition", "promoted_issue",
                "promotion_id", "material_event_id", "material_revision", "material_remote_id",
            }:
                raise ValueError(f"processed_promotion_schema_invalid:{event_id}")
            validate_output_envelope(PROCESSED_MARKER, disposition)
            if (
                type(disposition.get("schema_version")) is not int
                or disposition.get("schema_version") != 1
                or not isinstance(disposition.get("processed_at"), str)
                or not disposition["processed_at"]
                or not isinstance(disposition.get("material_revision"), int)
                or isinstance(disposition.get("material_revision"), bool)
                or disposition["material_revision"] <= 0
                or not isinstance(disposition.get("material_remote_id"), str)
                or not disposition["material_remote_id"]
            ):
                raise ValueError(f"processed_promotion_value_invalid:{event_id}")
            if (
                not promotion or disposition.get("promotion_id") != promotion.get("promotion_id")
                or disposition.get("promoted_issue") != promotion.get("workstream_id")
                or disposition.get("material_event_id") != promotion_delta(promotion).event_id
            ):
                raise ValueError(f"processed_without_promotion:{event_id}")
        elif promotion:
            raise ValueError(f"promotion_disposition_mismatch:{event_id}")
    return {
        "capture": capture,
        "promotion": promotion,
        "processed": None if legacy_disposition else disposition,
    }


def remote_issue_comments(repo: str, issue: int) -> list[dict[str, Any]]:
    pages = gh([
        "api", "--paginate", "--slurp", f"repos/{repo}/issues/{issue}/comments?per_page=100",
    ], timeout=20)
    if not pages:
        return []
    return [comment for page in pages for comment in page] if isinstance(pages[0], list) else pages


def remote_event_state(repo: str, issue: int, event_id: str) -> dict[str, Any]:
    canonical_ingress_route(repo, issue)
    return reduce_ingress_comments(
        remote_issue_comments(repo, issue), event_id=event_id, repo=repo, issue=issue,
    )


def append_ingress_marker(repo: str, issue: int, marker: str, payload: dict[str, Any]) -> Any:
    canonical_ingress_route(repo, issue)
    validate_output_envelope(marker, payload)
    return gh(
        ["api", f"repos/{repo}/issues/{issue}/comments", "--input", "-"],
        stdin=json.dumps({"body": comment_body(marker, payload)}), timeout=10,
    )


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
    canonical_repo = canonical_ingress_route(repo, 1)["repository"]
    issues = gh([
        "issue", "list", "--repo", repo, "--state", "all", "--label", LABEL,
        "--limit", "100", "--json", "number,url,title",
    ], timeout=15)
    captures: dict[str, dict[str, Any]] = {}
    processed: dict[str, dict[str, Any]] = {}
    mutable_hints: dict[tuple[int, str], dict[str, Any]] = {}
    promotions: dict[str, dict[str, Any]] = {}
    bindings: dict[str, dict[str, Any]] = {}
    event_routes: dict[str, dict[str, Any]] = {}
    comments_by_issue: dict[int, list[dict[str, Any]]] = {}

    def bind_route(event_id: str, issue_number: int) -> None:
        route = canonical_ingress_route(canonical_repo, issue_number)
        previous = event_routes.get(event_id)
        if previous and previous != route:
            raise ValueError(f"ingress_event_route_collision:{event_id}")
        event_routes[event_id] = route

    def record_mutable_hint(
        item: Any, comment: dict[str, Any], issue_number: int,
    ) -> None:
        if not isinstance(item, dict):
            return
        disposition = item.get("disposition")
        event_id = item.get("event_id")
        if (
            (
                disposition not in {"no-material-delta", "superseded"}
                and not _is_exact_legacy_processed_hint(item)
            )
            or not isinstance(event_id, str)
            or not event_id
            or len(event_id.encode("utf-8")) > 256
        ):
            return
        key = (issue_number, event_id)
        if key not in mutable_hints:
            if len(mutable_hints) >= MAX_CLASSIFICATION_HINT_GROUPS:
                return
            mutable_hints[key] = {
                "count": 0, "dispositions": set(), "fingerprints": set(),
                "ambiguous": False,
            }
        hint = mutable_hints[key]
        hint["count"] += 1
        hint["dispositions"].add(disposition)
        user = comment.get("user")
        user = user if isinstance(user, dict) else {}
        fingerprint_material = canonical_json({
            "body": comment.get("body") if isinstance(comment.get("body"), str) else "",
            "user_login": user.get("login") if isinstance(user.get("login"), str) else None,
            "user_id": user.get("id")
            if isinstance(user.get("id"), (str, int)) and not isinstance(user.get("id"), bool)
            else None,
            "created_at": comment.get("created_at")
            if isinstance(comment.get("created_at"), str) else None,
            "updated_at": comment.get("updated_at")
            if isinstance(comment.get("updated_at"), str) else None,
        }).encode("utf-8")
        fingerprint = hashlib.sha256(fingerprint_material).hexdigest()
        if fingerprint not in hint["fingerprints"]:
            if len(hint["fingerprints"]) < 2:
                hint["fingerprints"].add(fingerprint)
            else:
                hint["ambiguous"] = True
        if len(hint["fingerprints"]) > 1 or len(hint["dispositions"]) > 1:
            hint["ambiguous"] = True

    for issue in issues:
        issue_number = issue.get("number")
        canonical_ingress_route(canonical_repo, issue_number)
        pages = gh([
            "api", "--paginate", "--slurp",
            f"repos/{repo}/issues/{issue_number}/comments?per_page=100",
        ], timeout=20)
        comments = [comment for page in pages for comment in page] if pages and isinstance(pages[0], list) else pages
        comments_by_issue[issue_number] = comments
        for comment in comments:
            item = parse_comment(comment.get("body", ""), CAPTURE_MARKER)
            if item:
                bind_route(item["event_id"], issue_number)
                item["remote_issue"] = issue_number
                item["remote_repo"] = repo
                item["remote_url"] = comment.get("html_url")
                captures[item["event_id"]] = item
                continue
            item = parse_comment(comment.get("body", ""), PROCESSED_MARKER)
            if item:
                record_mutable_hint(item, comment, issue_number)
                if _is_exact_legacy_processed_hint(item):
                    continue
                if not isinstance(item, dict) or item.get("disposition") != "promoted":
                    continue
                event_id = item.get("event_id")
                if not isinstance(event_id, str) or not event_id:
                    raise ValueError("processed_promotion_schema_invalid:unknown")
                bind_route(event_id, issue_number)
                previous = processed.get(event_id)
                if previous and _payload_without_time(previous) != _payload_without_time(item):
                    raise ValueError(f"conflicting_processed:{event_id}")
                processed[event_id] = item
                continue
            item = parse_comment(comment.get("body", ""), PROMOTION_MARKER)
            if item:
                bind_route(item["event_id"], issue_number)
                validate_promotion_payload(item)
                if item["ingress_route"] != canonical_ingress_route(canonical_repo, issue_number):
                    raise ValueError(f"promotion_ingress_route_mismatch:{item['event_id']}")
                previous = promotions.get(item["event_id"])
                if previous and previous != item:
                    raise ValueError(f"conflicting_promotion:{item['event_id']}")
                promotions[item["event_id"]] = item
                continue
            item = parse_comment(comment.get("body", ""), BIND_MARKER)
            if item:
                bind_route(item["event_id"], issue_number)
                bindings[item["event_id"]] = item
    for event_id, binding in bindings.items():
        if event_id in captures:
            captures[event_id]["workstream_id"] = binding.get("workstream_id")
            captures[event_id]["context_url"] = binding.get("context_url")
    for event_id, event in captures.items():
        route = event_routes[event_id]
        hint = mutable_hints.get((route["issue"], event_id))
        if hint:
            count = hint["count"]
            event["classification_hint"] = {
                "authoritative": False,
                "reason": "mutable_github_comment",
                "observed_count": min(count, MAX_CLASSIFICATION_HINTS_REPORTED),
                "additional_hints_omitted": max(
                    0, count - MAX_CLASSIFICATION_HINTS_REPORTED,
                ),
                "dispositions": sorted(hint["dispositions"]),
                "ambiguous": bool(hint["ambiguous"]),
            }
    for event_id in processed:
        if event_id not in captures:
            raise ValueError(f"processed_without_capture:{event_id}")
    for event_id, promotion in promotions.items():
        if event_id not in captures:
            raise ValueError(f"promotion_without_capture:{event_id}")
        route = event_routes[event_id]
        staged = reduce_ingress_comments(
            comments_by_issue[route["issue"]], event_id=event_id,
            repo=route["repository"], issue=route["issue"],
        )
        if staged["promotion"] != promotion:
            raise ValueError(f"promotion_state_mismatch:{event_id}")
        captures[event_id]["promotion"] = promotion
        captures[event_id]["promotion_state"] = "staged"
    candidates = captures.items()
    if workstream:
        candidates = [item for item in candidates if item[1].get("workstream_id") == workstream]
    verified_processed: set[str] = set()
    for event_id, event in candidates:
        disposition = processed.get(event_id)
        if not disposition:
            continue
        if disposition.get("disposition") != "promoted":
            raise ValueError(f"processed_disposition_invalid:{event_id}")
        route = event_routes[event_id]
        state = reduce_ingress_comments(
            comments_by_issue[route["issue"]], event_id=event_id,
            repo=route["repository"], issue=route["issue"],
        )
        promotion = state["promotion"]
        if not promotion:
            raise ValueError(f"processed_without_promotion:{event_id}")
        adapter = linear_adapter_for_promotion(promotion)
        verify_processed_promotion(adapter, promotion_delta(promotion), disposition)
        verified_processed.add(event_id)
    result = [event for event_id, event in candidates if event_id not in verified_processed]
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
    surface = args.surface or (None if event or session else (
        os.environ.get("WORKSTREAM_SURFACE_ID") or os.environ.get("CMUX_SURFACE_ID")
    ))
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
            "bind requires an explicit --event/--session or a trusted adapter surface; "
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


def drain_pending(
    conn: sqlite3.Connection, config: dict[str, Any], exclude: str, limit: int
) -> int:
    """Upload up to `limit` older unacknowledged rows, oldest first.

    Stops at the first failure: if the remote just started refusing, the rest
    of the backlog will fail the same way and capture must not spend the user's
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
            raise ValueError("capture payload must be an object")
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
            # notices and runs `flush`.
            drain_pending(conn, config, event["event_id"], OPPORTUNISTIC_DRAIN)
        enforce_bound(conn, config)
    except Exception as error:
        # An observability capture must never block the user's agent prompt.
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


def command_flush(args: argparse.Namespace) -> int:
    """Upload every unacknowledged row, stopping at the first refusal.

    Stopping is correct — if the remote just refused, the rest of the backlog
    will refuse the same way and hammering it makes things worse. Stopping
    SILENTLY is not: a flush that printed `{"pending_before": 58, "uploaded": 0}`
    and nothing else was indistinguishable from a flush with nothing to do, and
    the operator had to go read failures.jsonl separately to learn that GitHub
    was returning 503s, so the sanitized reason is both recorded and reported.
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
            stopped_detail, _, _ = redact_text(str(error), max_bytes=200)
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
    if args.disposition == "promoted":
        raise ValueError(
            "material promotion requires the receipt-verifying promote command"
        )
    raise ValueError(
        "non_material_classification_not_durable: GitHub comments are mutable hints; "
        "leave the capture open until an immutable classification receipt exists"
    )


def command_promote(args: argparse.Namespace) -> int:
    """Durably stage, apply, and acknowledge one reviewed ingress promotion.

    A request is needed only for the first attempt. Once its immutable intent
    marker exists, a successor machine can resume from repo/issue/event alone.
    """
    if (
        not isinstance(args.max_conflicts, int) or isinstance(args.max_conflicts, bool)
        or not 0 <= args.max_conflicts <= MAX_PROMOTION_CONFLICTS
    ):
        raise ValueError(f"promotion_max_conflicts_must_be_0_to_{MAX_PROMOTION_CONFLICTS}")
    request = load_promotion_request(args.request) if args.request else None
    config = load_config()
    if request:
        ingress = request["ingress"]
        repo, issue, event_id = ingress["repo"], ingress["remote_issue"], ingress["event_id"]
        if args.repo and args.repo != repo:
            raise ValueError("promotion_repo_mismatch")
        if args.remote_issue and args.remote_issue != issue:
            raise ValueError("promotion_issue_mismatch")
        if args.event and args.event != event_id:
            raise ValueError("promotion_event_mismatch")
    else:
        repo = args.repo or config.get("repo")
        issue = args.remote_issue or config.get("issue")
        event_id = args.event
        if not repo or not issue or not event_id:
            raise ValueError(
                "promotion recovery requires --repo, --remote-issue, and --event "
                "(repo/issue may come from local config)"
            )

    state = remote_event_state(repo, int(issue), event_id)
    capture = state["capture"]
    if not capture:
        raise ValueError(f"ingress_capture_not_found:{event_id}")
    if request:
        if capture.get("prompt_sha256") != request["ingress"]["prompt_sha256"]:
            raise ValueError("promotion_prompt_sha256_mismatch")
        if capture.get("workstream_id") != request["workstream_id"]:
            raise ValueError("promotion_workstream_mismatch")
        proposed = promotion_payload(request, capture)
        validate_promotion_payload(proposed)
        if state["promotion"] and state["promotion"] != proposed:
            raise ValueError(f"conflicting_promotion:{event_id}")
        promotion = state["promotion"] or proposed
    else:
        promotion = state["promotion"]
        if not promotion:
            raise ValueError(
                f"promotion_intent_missing:{event_id}; supply the reviewed --request once"
            )

    delta = promotion_delta(promotion)
    processed = state["processed"]
    if processed:
        if (
            processed.get("disposition") != "promoted"
            or processed.get("promotion_id") != promotion["promotion_id"]
            or processed.get("material_event_id") != delta.event_id
        ):
            raise ValueError(f"conflicting_processed:{event_id}")
        adapter = linear_adapter_for_promotion(promotion, config_path=args.config)
        verify_processed_promotion(adapter, delta, processed)
        _record_local_processed(event_id, processed)
        print(json.dumps({
            "event_id": event_id, "promotion_id": promotion["promotion_id"],
            "material_event_id": delta.event_id, "disposition": "promoted",
            "replay": True,
        }, sort_keys=True))
        return 0

    if not args.apply:
        print(json.dumps({
            "event_id": event_id, "promotion_id": promotion["promotion_id"],
            "material_event_id": delta.event_id, "workstream_id": promotion["workstream_id"],
            "plan_revision": promotion["plan_revision"],
            "expected_material_revision": promotion["expected_material_revision"],
            "changes": len(promotion["changes"]), "would_stage": state["promotion"] is None,
            "would_apply": True,
        }, indent=2, sort_keys=True))
        return 0

    if state["promotion"] is None:
        try:
            append_ingress_marker(repo, int(issue), PROMOTION_MARKER, promotion)
        except Exception:
            # A lost response is indistinguishable from a failed request until
            # authoritative readback. Accept only the exact staged intent.
            reread = remote_event_state(repo, int(issue), event_id)
            if reread["promotion"] != promotion:
                raise
        state = remote_event_state(repo, int(issue), event_id)
        if state["promotion"] != promotion:
            raise ValueError("promotion_stage_not_observed")
    promotion_failpoint("after_stage")

    adapter = linear_adapter_for_promotion(promotion, config_path=args.config)
    receipt = apply_promotion_delta(adapter, delta, max_conflicts=args.max_conflicts)
    promotion_failpoint("after_linear")
    processed_payload = {
        "schema_version": 1,
        "event_id": event_id,
        "processed_at": utc_now(),
        "disposition": "promoted",
        "promoted_issue": promotion["workstream_id"],
        "promotion_id": promotion["promotion_id"],
        "material_event_id": delta.event_id,
        "material_revision": receipt.revision,
        "material_remote_id": receipt.remote_id,
    }
    try:
        append_ingress_marker(repo, int(issue), PROCESSED_MARKER, processed_payload)
    except Exception:
        reread = remote_event_state(repo, int(issue), event_id)
        observed = reread["processed"]
        if not observed or _payload_without_time(observed) != _payload_without_time(processed_payload):
            raise
    final = remote_event_state(repo, int(issue), event_id)
    observed = final["processed"]
    if not observed or _payload_without_time(observed) != _payload_without_time(processed_payload):
        raise ValueError("promotion_processed_not_observed")
    promotion_failpoint("after_processed")
    _record_local_processed(event_id, observed)
    print(json.dumps({
        "event_id": event_id, "promotion_id": promotion["promotion_id"],
        "material_event_id": delta.event_id, "material_revision": receipt.revision,
        "material_remote_id": receipt.remote_id, "disposition": "promoted", "replay": False,
    }, sort_keys=True))
    return 0


def _record_local_processed(event_id: str, payload: dict[str, Any]) -> None:
    """Best-effort local cache update after remote successor proof exists."""
    try:
        conn = connect()
        conn.execute(
            "UPDATE events SET processed_at=?,disposition=?,promoted_issue=? WHERE event_id=?",
            (payload.get("processed_at") or utc_now(), payload.get("disposition"),
             payload.get("promoted_issue"), event_id),
        )
        conn.commit()
        conn.close()
    except (OSError, sqlite3.Error) as error:
        # The authoritative processed marker and Linear receipt already exist.
        # A successor machine may have no writable local ingress cache at all.
        record_failure("local-processed-cache", error)


def command_status(args: argparse.Namespace) -> int:
    conn = connect()
    counts = conn.execute(
        "SELECT COUNT(*), SUM(remote_acked_at IS NULL), SUM(processed_at IS NULL) FROM events"
    ).fetchone()
    # Unbound volume is the number that matters and the one nobody was
    # watching: an unbound event is an open obligation no recovery pass can
    # find by workstream.
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
    process.add_argument("--disposition", choices=("no-material-delta", "superseded"), required=True)
    process.add_argument("--issue")
    process.set_defaults(func=command_process)
    promote = sub.add_parser("promote")
    promote.add_argument("--request", help="reviewed bounded JSON request, or - for stdin")
    promote.add_argument("--repo")
    promote.add_argument("--remote-issue", type=int)
    promote.add_argument("--event")
    promote.add_argument("--config", help="explicit .workstream.json route")
    promote.add_argument("--max-conflicts", type=int, default=MAX_PROMOTION_CONFLICTS)
    promote.add_argument("--apply", action="store_true")
    promote.set_defaults(func=command_promote)
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
