#!/usr/bin/env python3
"""Crash-consistent, model-free material-delta journal.

The journal is deliberately independent of a particular Linear transport. An
adapter implements ``apply(delta)`` and must make the remote mutation
idempotent by ``event_id``. Mutable-state transports must enforce
``expected_revision`` with atomic CAS; append-only transports may accept
concurrent events based on the same revision because neither replaces the
other. The local transaction is the durable hand-off: a process may die after
the adapter accepts a mutation but before the journal records it as applied,
so replay is expected and safe.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class Delta:
    event_id: str
    workstream_id: str
    kind: str
    source: str
    payload: dict[str, Any]
    expected_revision: int
    created_at: str


@dataclass(frozen=True)
class MutationReceipt:
    event_id: str
    revision: int
    remote_id: str


class RevisionConflict(RuntimeError):
    """The remote root changed; caller must reread and replay, never overwrite."""


class RemoteCASUnavailable(RuntimeError):
    """The adapter offers neither remote CAS nor a lossless append-only log."""


class MutationAdapter(Protocol):
    supports_atomic_cas: bool
    supports_append_only_events: bool

    def apply(self, delta: Delta) -> MutationReceipt:
        """Apply once by event_id, or raise RevisionConflict."""

    def current_revision(self, workstream_id: str) -> int:
        """Return the live root revision after a conflict."""


def _supports_lossless_remote_mutation(adapter: MutationAdapter) -> bool:
    """Accept either fenced state replacement or an atomic append-only log.

    An append-only event transport does not need compare-and-swap because two
    writers never replace the same state. Its reducer must derive revision
    from the complete durable event set and reject duplicate event IDs.
    """
    return (
        getattr(adapter, "supports_atomic_cas", False) is True
        or getattr(adapter, "supports_append_only_events", False) is True
    )


def event_id_for(
    workstream_id: str,
    kind: str,
    payload: dict[str, Any],
    expected_revision: int,
    *,
    source: str = "user_turn",
) -> str:
    identity = [workstream_id, kind, expected_revision, payload]
    if source != "user_turn":
        identity = [workstream_id, kind, source, expected_revision, payload]
    material = json.dumps(
        identity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "wsd_" + hashlib.sha256(material).hexdigest()[:32]


class DeltaJournal:
    """SQLite outbox whose pending rows are never acknowledged early."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=2)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=2000")
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS material_deltas (
              event_id TEXT PRIMARY KEY,
              workstream_id TEXT NOT NULL,
              kind TEXT NOT NULL,
              source TEXT NOT NULL DEFAULT 'user_turn',
              payload TEXT NOT NULL,
              expected_revision INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              applied_at TEXT,
              remote_id TEXT,
              applied_revision INTEGER
            )
            """
        )
        columns = {
            row[1] for row in self.db.execute("PRAGMA table_info(material_deltas)")
        }
        if "source" not in columns:
            self.db.execute(
                "ALTER TABLE material_deltas ADD COLUMN source TEXT NOT NULL DEFAULT 'user_turn'"
            )
        self.commit_hook = None
        self._commit()

    def _commit(self) -> None:
        """Commit through an injectable failpoint used by crash tests."""
        if self.commit_hook:
            self.commit_hook()
        self.db.commit()

    def append(
        self,
        workstream_id: str,
        kind: str,
        payload: dict[str, Any],
        expected_revision: int,
        *,
        event_id: str | None = None,
        source: str = "user_turn",
    ) -> str:
        if not workstream_id or not kind:
            raise ValueError("workstream_id and kind are required")
        if source not in {"user_turn", "agent_discovery", "checkpoint", "system"}:
            raise ValueError(f"unsupported material-delta source: {source}")
        if expected_revision < 0:
            raise ValueError("expected_revision must be non-negative")
        event_id = event_id or event_id_for(
            workstream_id, kind, payload, expected_revision, source=source
        )
        payload_json = json.dumps(payload, sort_keys=True)
        self.db.execute(
            """INSERT OR IGNORE INTO material_deltas
               (event_id,workstream_id,kind,source,payload,expected_revision,created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (
                event_id,
                workstream_id,
                kind,
                source,
                payload_json,
                expected_revision,
                now(),
            ),
        )
        stored = self.db.execute(
            """SELECT workstream_id,kind,source,payload,expected_revision
               FROM material_deltas WHERE event_id=?""",
            (event_id,),
        ).fetchone()
        if stored != (workstream_id, kind, source, payload_json, expected_revision):
            raise ValueError(f"event_id_collision:{event_id}")
        self._commit()
        return event_id

    def append_boundary(
        self,
        workstream_id: str,
        boundary_id: str,
        changes: list[dict[str, Any]],
        expected_revision: int,
        *,
        source: str = "user_turn",
        checkpoint: dict[str, Any] | None = None,
    ) -> str | None:
        """Journal one substantive turn as one idempotent mutation batch.

        An empty boundary is deliberately a no-op: routine turns must not churn
        the ledger merely to record that nothing material changed.
        """
        if not changes:
            return None
        if not boundary_id:
            raise ValueError("boundary_id is required")
        for change in changes:
            if not isinstance(change, dict) or not change.get("kind") or "payload" not in change:
                raise ValueError("each boundary change needs kind and payload")
        payload: dict[str, Any] = {
            "boundary_id": boundary_id,
            "changes": changes,
        }
        if checkpoint is not None:
            payload["checkpoint"] = checkpoint
        return self.append(
            workstream_id,
            "material_boundary",
            payload,
            expected_revision,
            source=source,
        )

    def pending(self, limit: int = 100) -> list[Delta]:
        rows = self.db.execute(
            """SELECT event_id,workstream_id,kind,source,payload,expected_revision,created_at
               FROM material_deltas WHERE applied_at IS NULL
               ORDER BY created_at,event_id LIMIT ?""",
            (limit,),
        ).fetchall()
        return [
            Delta(event_id, workstream, kind, source, json.loads(payload), revision, created)
            for event_id, workstream, kind, source, payload, revision, created in rows
        ]

    def _acknowledge(self, delta: Delta, receipt: MutationReceipt) -> None:
        if receipt.event_id != delta.event_id:
            raise ValueError("adapter receipt event_id mismatch")
        if receipt.revision <= delta.expected_revision:
            raise ValueError("adapter receipt did not advance the root revision")
        self.db.execute(
            """UPDATE material_deltas
               SET applied_at=?, remote_id=?, applied_revision=?
               WHERE event_id=? AND applied_at IS NULL""",
            (now(), receipt.remote_id, receipt.revision, delta.event_id),
        )
        self._commit()

    def apply(self, adapter: MutationAdapter, limit: int = 100) -> list[MutationReceipt]:
        """Apply pending rows and acknowledge only after adapter success.

        If the process dies after ``adapter.apply`` returns, the row remains
        pending and is replayed.  Adapters must deduplicate by event_id; this
        is the required crash window, not an exceptional path.
        """
        if not _supports_lossless_remote_mutation(adapter):
            raise RemoteCASUnavailable("remote_cas_unavailable")
        receipts: list[MutationReceipt] = []
        for delta in self.pending(limit):
            receipt = adapter.apply(delta)
            self._acknowledge(delta, receipt)
            receipts.append(receipt)
        return receipts

    def apply_with_rebase(
        self,
        adapter: MutationAdapter,
        limit: int = 100,
        *,
        max_conflicts: int = 8,
    ) -> list[MutationReceipt]:
        """Apply with a lossless remote primitive and bounded conflict replay.

        The event ID remains stable when its expected revision is rebased. A
        best-effort read/write adapter is refused before mutation: read-after-
        write verification cannot turn Linear's non-conditional update into a
        compare-and-swap. An append-only adapter does not rebase ordinary
        concurrent events because a shared expected revision is valid there.
        """
        if not _supports_lossless_remote_mutation(adapter):
            raise RemoteCASUnavailable("remote_cas_unavailable")
        if max_conflicts < 0:
            raise ValueError("max_conflicts must be non-negative")
        receipts: list[MutationReceipt] = []
        for original in self.pending(limit):
            delta = original
            conflicts = 0
            while True:
                try:
                    receipt = adapter.apply(delta)
                    break
                except RevisionConflict:
                    if conflicts >= max_conflicts:
                        raise
                    live_revision = adapter.current_revision(delta.workstream_id)
                    if live_revision <= delta.expected_revision:
                        raise RevisionConflict(
                            "conflict did not expose a newer live revision"
                        )
                    delta = Delta(
                        delta.event_id,
                        delta.workstream_id,
                        delta.kind,
                        delta.source,
                        delta.payload,
                        live_revision,
                        delta.created_at,
                    )
                    conflicts += 1
            self._acknowledge(delta, receipt)
            receipts.append(receipt)
        return receipts

    def close(self) -> None:
        self.db.close()
