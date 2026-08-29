#!/usr/bin/env python3
"""Typed, append-only decision events and their deterministic current view.

This is the transport-neutral logical core.  Persistence adapters may carry
these payloads, but this module does not claim that Linear resume currently
fetches them.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json
from typing import Any

from workstream_scope import (
    canonical_repository, is_full_oid, is_issue_token, is_namespace, ScopeError,
)


SCHEMA_VERSION = 1
EVENT_TYPES = {"recorded", "audited", "superseded"}
VERDICTS = {"accepted", "provisional", "must_fix"}
REACH = {"local", "component", "system", "cross_system", "fleet"}
CONFIDENCE = {"low", "medium", "high"}
HIGH_RISK_DOMAINS = {
    "security", "authority", "persistence", "concurrency", "release", "fleet"
}
DOMAINS = HIGH_RISK_DOMAINS | {
    "compatibility", "cost", "maintainability", "performance",
    "product_behavior", "user_experience", "other",
}


class ChoiceError(ValueError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _event_id(event: dict[str, Any]) -> str:
    material = {key: value for key, value in event.items() if key != "event_id"}
    return "wsc_choice_" + hashlib.sha256(_canonical(material)).hexdigest()[:32]


def _base(*, event_type: str, choice_id: str, workstream_id: str,
          owning_child: str, namespace: str, repository: str, repository_key: str,
          plan_revision: str, git_head: str,
          created_at: str, payload: dict[str, Any]) -> dict[str, Any]:
    event = {
        "schema_version": SCHEMA_VERSION,
        "event_type": event_type,
        "choice_id": choice_id,
        "workstream_id": workstream_id.upper(),
        "owning_child": owning_child.upper(),
        "namespace": namespace,
        "repository": repository,
        "repository_key": repository_key,
        "plan_revision": plan_revision,
        "git_head": git_head.lower(),
        "created_at": created_at,
        "payload": deepcopy(payload),
    }
    event["event_id"] = _event_id(event)
    validate_event(event)
    return event


def record_choice(*, choice_id: str, workstream_id: str, owning_child: str,
                  namespace: str, repository: str, repository_key: str,
                  plan_revision: str,
                  git_head: str, created_at: str,
                  spec_gap: str, decision: str, alternatives: list[str],
                  reach: str, irreversible: bool, domains: list[str],
                  technical_confidence: str, intent_confidence: str,
                  acceptance_criteria: list[str] | None = None,
                  evidence: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    payload = {
        "spec_gap": spec_gap,
        "decision": decision,
        "alternatives": deepcopy(alternatives),
        "reach": reach,
        "irreversible": irreversible,
        "domains": sorted(set(domains)),
        "technical_confidence": technical_confidence,
        "intent_confidence": intent_confidence,
        "evidence": deepcopy(evidence or []),
    }
    if acceptance_criteria is not None:
        payload["acceptance_criteria"] = deepcopy(acceptance_criteria)
    return _base(
        event_type="recorded", choice_id=choice_id,
        workstream_id=workstream_id, owning_child=owning_child,
        namespace=namespace, repository=repository,
        repository_key=repository_key,
        plan_revision=plan_revision, git_head=git_head, created_at=created_at,
        payload=payload,
    )


def audit_choice(*, choice_id: str, workstream_id: str, owning_child: str,
                 namespace: str, repository: str, repository_key: str,
                 plan_revision: str,
                 git_head: str, created_at: str,
                 recorded_event_id: str, verdict: str, rationale: str,
                 auditor: str, evidence: list[dict[str, Any]] | None = None,
                 fresh_context: bool = True, read_only: bool = True) -> dict[str, Any]:
    return _base(
        event_type="audited", choice_id=choice_id,
        workstream_id=workstream_id, owning_child=owning_child,
        namespace=namespace, repository=repository,
        repository_key=repository_key,
        plan_revision=plan_revision, git_head=git_head, created_at=created_at,
        payload={
            "recorded_event_id": recorded_event_id,
            "verdict": verdict,
            "rationale": rationale,
            "auditor": auditor,
            "fresh_context": fresh_context,
            "read_only": read_only,
            "evidence": deepcopy(evidence or []),
        },
    )


def supersede_choice(*, choice_id: str, workstream_id: str, owning_child: str,
                     namespace: str, repository: str, repository_key: str,
                     plan_revision: str,
                     git_head: str, created_at: str,
                     target_event_id: str, reason: str,
                     successor_choice_id: str | None = None) -> dict[str, Any]:
    return _base(
        event_type="superseded", choice_id=choice_id,
        workstream_id=workstream_id, owning_child=owning_child,
        namespace=namespace, repository=repository,
        repository_key=repository_key,
        plan_revision=plan_revision, git_head=git_head, created_at=created_at,
        payload={
            "target_event_id": target_event_id,
            "reason": reason,
            "successor_choice_id": successor_choice_id,
        },
    )


def _required_text(value: Any, error: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ChoiceError(error)


def _parse_timestamp(value: Any) -> datetime:
    _required_text(value, "missing_created_at")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ChoiceError("invalid_created_at") from error
    if parsed.tzinfo is None:
        raise ChoiceError("created_at_requires_timezone")
    return parsed


def validate_event(event: dict[str, Any]) -> None:
    if event.get("schema_version") != SCHEMA_VERSION:
        raise ChoiceError("unsupported_choice_schema")
    if event.get("event_type") not in EVENT_TYPES:
        raise ChoiceError("invalid_choice_event_type")
    _required_text(event.get("choice_id"), "missing_choice_id")
    for field in ("workstream_id", "owning_child"):
        value = event.get(field)
        if not is_issue_token(value):
            raise ChoiceError(f"invalid_{field}")
    if not is_namespace(event.get("namespace")):
        raise ChoiceError("invalid_namespace")
    if not isinstance(event.get("repository"), str):
        raise ChoiceError("invalid_repository")
    try:
        if canonical_repository(event["repository"]) != event["repository"]:
            raise ChoiceError("repository_not_canonical")
    except ScopeError as error:
        raise ChoiceError("invalid_repository") from error
    _required_text(event.get("repository_key"), "missing_repository_key")
    if not event["repository_key"].startswith(event["repository"].split("/", 1)[0] + ":"):
        raise ChoiceError("repository_key_host_mismatch")
    _required_text(event.get("plan_revision"), "missing_plan_revision")
    if not is_full_oid(event.get("git_head")):
        raise ChoiceError("invalid_git_head")
    _parse_timestamp(event.get("created_at"))
    payload = event.get("payload")
    if not isinstance(payload, dict):
        raise ChoiceError("invalid_choice_payload")
    event_type = event["event_type"]
    if event_type == "recorded":
        for field in ("spec_gap", "decision"):
            _required_text(payload.get(field), f"missing_{field}")
        if not isinstance(payload.get("alternatives"), list):
            raise ChoiceError("invalid_alternatives")
        if payload.get("reach") not in REACH:
            raise ChoiceError("invalid_reach")
        if not isinstance(payload.get("irreversible"), bool):
            raise ChoiceError("invalid_irreversible")
        domains = payload.get("domains")
        if not isinstance(domains, list) or not all(isinstance(item, str) for item in domains):
            raise ChoiceError("invalid_domains")
        unknown_domains = set(domains) - DOMAINS
        if unknown_domains:
            raise ChoiceError("unknown_domains:" + ",".join(sorted(unknown_domains)))
        for field in ("technical_confidence", "intent_confidence"):
            if payload.get(field) not in CONFIDENCE:
                raise ChoiceError(f"invalid_{field}")
        criteria = payload.get("acceptance_criteria")
        if criteria is not None and (
            not isinstance(criteria, list)
            or not all(isinstance(item, str) and item.strip() for item in criteria)
            or len(criteria) != len(set(criteria))
        ):
            raise ChoiceError("invalid_acceptance_criteria")
    elif event_type == "audited":
        _required_text(payload.get("recorded_event_id"), "missing_recorded_event_id")
        _required_text(payload.get("rationale"), "missing_audit_rationale")
        _required_text(payload.get("auditor"), "missing_auditor")
        if payload.get("verdict") not in VERDICTS:
            raise ChoiceError("invalid_audit_verdict")
        if payload.get("fresh_context") is not True or payload.get("read_only") is not True:
            raise ChoiceError("audit_must_be_fresh_context_read_only")
    else:
        _required_text(payload.get("target_event_id"), "missing_target_event_id")
        _required_text(payload.get("reason"), "missing_supersession_reason")
        successor = payload.get("successor_choice_id")
        if successor is not None:
            _required_text(successor, "invalid_successor_choice_id")
            if successor == event["choice_id"]:
                raise ChoiceError("choice_cannot_supersede_itself")
    if event.get("event_id") != _event_id(event):
        raise ChoiceError("choice_event_id_mismatch")


def reduce_choices(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Validate immutable events and derive one current view per choice."""
    by_event: dict[str, dict[str, Any]] = {}
    for raw in events:
        event = deepcopy(raw)
        validate_event(event)
        event_id = event["event_id"]
        if event_id in by_event:
            if _canonical(by_event[event_id]) != _canonical(event):
                raise ChoiceError(f"choice_event_collision:{event_id}")
            continue
        by_event[event_id] = event

    records: dict[str, dict[str, Any]] = {}
    audits: dict[str, list[dict[str, Any]]] = {}
    superseded: dict[str, dict[str, Any]] = {}
    for event in by_event.values():
        choice_id = event["choice_id"]
        if event["event_type"] == "recorded":
            if choice_id in records:
                raise ChoiceError(f"multiple_choice_records:{choice_id}")
            records[choice_id] = event
        elif event["event_type"] == "audited":
            audits.setdefault(choice_id, []).append(event)
        else:
            if choice_id in superseded:
                raise ChoiceError(f"multiple_choice_supersessions:{choice_id}")
            superseded[choice_id] = event

    result: dict[str, dict[str, Any]] = {}
    for choice_id, record in records.items():
        related_audits = sorted(
            audits.pop(choice_id, []),
            key=lambda item: (_parse_timestamp(item["created_at"]), item["event_id"]),
        )
        for audit in related_audits:
            if audit["payload"]["recorded_event_id"] != record["event_id"]:
                raise ChoiceError(f"audit_target_mismatch:{choice_id}")
            if any(audit[field] != record[field] for field in ("workstream_id", "owning_child", "namespace", "repository_key")):
                raise ChoiceError(f"choice_ownership_changed:{choice_id}")
            if _parse_timestamp(audit["created_at"]) < _parse_timestamp(record["created_at"]):
                raise ChoiceError(f"audit_precedes_record:{choice_id}")
        supersession = superseded.pop(choice_id, None)
        successor = None
        if supersession:
            if supersession["payload"]["target_event_id"] != record["event_id"]:
                raise ChoiceError(f"supersession_target_mismatch:{choice_id}")
            if any(supersession[field] != record[field] for field in ("workstream_id", "owning_child", "namespace", "repository_key")):
                raise ChoiceError(f"choice_ownership_changed:{choice_id}")
            if _parse_timestamp(supersession["created_at"]) < _parse_timestamp(record["created_at"]):
                raise ChoiceError(f"supersession_precedes_record:{choice_id}")
            successor_id = supersession["payload"].get("successor_choice_id")
            if successor_id:
                successor = records.get(successor_id)
                if successor is None:
                    raise ChoiceError(f"successor_choice_not_found:{choice_id}:{successor_id}")
                if any(successor[field] != record[field] for field in ("workstream_id", "owning_child", "namespace", "repository_key")):
                    raise ChoiceError(f"successor_choice_ownership_changed:{choice_id}")
        latest_audit = related_audits[-1] if related_audits else None
        risk = record["payload"]
        high_risk = risk["irreversible"] or bool(HIGH_RISK_DOMAINS.intersection(risk["domains"]))
        verdict = latest_audit["payload"]["verdict"] if latest_audit else None
        if high_risk and verdict == "provisional":
            raise ChoiceError(f"high_risk_choice_cannot_be_provisional:{choice_id}")
        successor_criteria = set(
            successor["payload"].get("acceptance_criteria") or []
        ) if successor is not None else set()
        result[choice_id] = {
            "record": record,
            "audits": related_audits,
            "supersession": supersession,
            "active": supersession is None,
            "high_risk": high_risk,
            "verdict": verdict,
            "retired_acceptance_criteria": [
                criterion
                for criterion in record["payload"].get("acceptance_criteria") or []
                if supersession is not None and criterion not in successor_criteria
            ],
            "landing_blocked": supersession is None and (
                latest_audit is None or verdict == "must_fix"
            ),
        }
    if audits:
        raise ChoiceError("audit_without_record:" + ",".join(sorted(audits)))
    if superseded:
        raise ChoiceError("supersession_without_record:" + ",".join(sorted(superseded)))
    return result


def retired_acceptance_criteria(events: list[dict[str, Any]]) -> dict[str, str]:
    """Map criteria removed by a choice supersession to the predecessor."""
    retired: dict[str, str] = {}
    for choice_id, view in reduce_choices(events).items():
        for criterion in view["retired_acceptance_criteria"]:
            previous = retired.setdefault(criterion, choice_id)
            if previous != choice_id:
                raise ChoiceError(f"acceptance_criterion_retired_twice:{criterion}")
    return retired


def closure_blockers(events: list[dict[str, Any]], *, plan_revision: str,
                     exact_head: str | None = None,
                     repository_heads: dict[str, str] | None = None,
                     workstream_id: str | None = None,
                     child_ids: set[str] | None = None) -> list[str]:
    """Require active decisions to be reconciled to closure truth."""
    blockers: list[str] = []
    for choice_id, view in reduce_choices(events).items():
        if not view["active"]:
            continue
        record = view["record"]
        if workstream_id is not None and record["workstream_id"] != workstream_id:
            blockers.append(f"choice_workstream_mismatch:{choice_id}")
        if child_ids is not None and record["owning_child"] not in child_ids:
            blockers.append(f"choice_owner_missing:{choice_id}")
        latest = view["audits"][-1] if view["audits"] else record
        if latest["plan_revision"] != plan_revision:
            blockers.append(f"choice_plan_drift:{choice_id}")
        expected_head = (
            repository_heads.get(record["repository_key"])
            if repository_heads is not None else exact_head
        )
        if expected_head is None:
            blockers.append(f"choice_repository_head_missing:{choice_id}")
        elif latest["git_head"] != expected_head.lower():
            blockers.append(f"choice_head_not_reconciled:{choice_id}")
        if view["landing_blocked"]:
            blockers.append(f"choice_landing_blocked:{choice_id}")
    return sorted(blockers)
