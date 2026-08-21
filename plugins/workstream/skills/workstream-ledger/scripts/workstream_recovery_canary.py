#!/usr/bin/env python3
"""Evaluate a recorded cross-machine recovery canary without live mutations.

The evaluator proves only that the supplied observation satisfies the physical
canary contract. Collection, authentication, and machine/process control remain
outside this transport-neutral module.
"""

from __future__ import annotations

from copy import deepcopy
import re
from typing import Any
from urllib.parse import urlparse

from workstream_checkpoint import CheckpointError, recover_latest


TOKEN = re.compile(r"^[A-Z][A-Z0-9]*-\d+$")


class RecoveryCanaryError(ValueError):
    pass


def _required_text(record: dict[str, Any], field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise RecoveryCanaryError(f"canary_missing:{field}")
    return value


def evaluate(record: dict[str, Any], checkpoints: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a bounded contract receipt or fail closed on any contradiction."""
    token = _required_text(record, "resume_token").upper()
    if not TOKEN.fullmatch(token):
        raise RecoveryCanaryError("invalid_resume_token")
    plan = record.get("canonical_plan") or {}
    plan_url = _required_text(plan, "url")
    plan_revision = _required_text(plan, "revision")
    if urlparse(plan_url).scheme != "https":
        raise RecoveryCanaryError("canonical_plan_url_required")
    root = record.get("root") or {}
    if _required_text(root, "identifier").upper() != token:
        raise RecoveryCanaryError("root_token_mismatch")
    context_url = _required_text(root, "context_url")
    parsed_context = urlparse(context_url)
    if parsed_context.scheme != "https" or parsed_context.hostname != "linear.app":
        raise RecoveryCanaryError("linear_root_context_required")
    if f"/{token.lower()}/" not in context_url.lower() and not context_url.lower().endswith(f"/{token.lower()}"):
        raise RecoveryCanaryError("root_context_token_mismatch")
    if not isinstance(root.get("revision"), int) or root["revision"] < 0:
        raise RecoveryCanaryError("invalid_root_revision")

    termination = record.get("source_termination") or {}
    if termination.get("phase") != "after_remote_ack_before_final_response":
        raise RecoveryCanaryError("source_death_not_after_remote_ack")
    if termination.get("process_unavailable") is not True:
        raise RecoveryCanaryError("source_process_not_proven_unavailable")

    try:
        recovered = recover_latest(checkpoints, token, expected_plan_revision=plan_revision)
    except CheckpointError as exc:
        raise RecoveryCanaryError(str(exc)) from exc
    if recovered["root_revision"] != root.get("revision"):
        raise RecoveryCanaryError("root_revision_mismatch")

    provenance = recovered["provenance_chain"]
    source = record.get("source") or {}
    observed_source = provenance[0]
    for field in ("machine", "session_id", "agent", "provider"):
        if source.get(field) != observed_source.get(field):
            raise RecoveryCanaryError(f"source_provenance_mismatch:{field}")
    recovery = record.get("recovery") or {}
    for field in ("machine", "session_id", "agent", "provider"):
        _required_text(recovery, field)
    if recovery["machine"] == source["machine"]:
        raise RecoveryCanaryError("recovery_machine_not_distinct")
    if recovery["session_id"] == source["session_id"]:
        raise RecoveryCanaryError("recovery_session_not_distinct")

    observation = record.get("remote_observation") or {}
    acknowledgement = recovered["acknowledgement"]
    expected = {
        "event_id": recovered["checkpoint_event_id"],
        "remote_id": acknowledgement["remote_id"],
        "applied_revision": acknowledgement["applied_revision"],
    }
    if observation != expected:
        raise RecoveryCanaryError("remote_ack_observation_mismatch")
    return {
        "result": "contract_pass",
        "resume_token": token,
        "canonical_plan": {"url": plan_url, "revision": plan_revision},
        "root": {"identifier": token, "context_url": context_url, "revision": root["revision"]},
        "source": deepcopy(source),
        "recovery": deepcopy(recovery),
        "remote_observation": deepcopy(expected),
        "next_action": recovered["next_action"],
        "evidence_scope": "supplied_observation_only",
        "live_mutations_performed": False,
    }
