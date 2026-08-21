#!/usr/bin/env python3
"""Executable validation for a layered, exact-head slice evidence contract."""

from __future__ import annotations

from typing import Any

from workstream_scope import (
    canonical_repository, is_full_oid, is_issue_token, ScopeError,
)


LAYERS = {
    "architecture", "logic", "component", "adapter", "e2e", "visual",
    "operational", "negative_control",
}
TEST_LAYERS = {"logic", "component", "adapter", "e2e", "negative_control"}


def _receipt_succeeded(receipt: Any, contract: dict[str, Any]) -> bool:
    if not isinstance(receipt, dict):
        return False
    outcome_ok = (
        receipt.get("passed") is True
        or receipt.get("verified") is True
        or receipt.get("status") in {"passed", "success", "accepted"}
        or receipt.get("outcome") in {"passed", "success", "accepted"}
    )
    return (
        outcome_ok
        and isinstance(receipt.get("kind"), str) and bool(receipt["kind"].strip())
        and isinstance(receipt.get("proof"), str) and bool(receipt["proof"].strip())
        and receipt.get("repository_key") == contract.get("repository_key")
        and receipt.get("exact_head") == contract.get("exact_head")
    )


def evidence_errors(contract: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in ("slice_id", "owning_child", "repository", "repository_key",
                  "plan_revision", "exact_head"):
        if not isinstance(contract.get(field), str) or not contract[field].strip():
            errors.append(f"missing:{field}")
    if isinstance(contract.get("exact_head"), str) and not is_full_oid(contract["exact_head"]):
        errors.append("invalid:exact_head")
    if isinstance(contract.get("owning_child"), str) and not is_issue_token(contract["owning_child"]):
        errors.append("invalid:owning_child")
    if isinstance(contract.get("repository"), str):
        try:
            if canonical_repository(contract["repository"]) != contract["repository"]:
                errors.append("invalid:repository_not_canonical")
        except ScopeError:
            errors.append("invalid:repository")
        if isinstance(contract.get("repository_key"), str) and not contract["repository_key"].startswith(
            contract["repository"].split("/", 1)[0] + ":"
        ):
            errors.append("invalid:repository_key_host_mismatch")
    layers = contract.get("layers")
    if not isinstance(layers, dict):
        return sorted(set(errors + ["missing:layers"]))
    errors.extend(f"missing_layer:{name}" for name in sorted(LAYERS - set(layers)))
    errors.extend(f"unknown_layer:{name}" for name in sorted(set(layers) - LAYERS))
    for name in sorted(LAYERS.intersection(layers)):
        layer = layers[name]
        if not isinstance(layer, dict) or layer.get("status") not in {"required", "not_applicable"}:
            errors.append(f"invalid_layer_status:{name}")
            continue
        if layer["status"] == "not_applicable":
            if not isinstance(layer.get("reason"), str) or not layer["reason"].strip():
                errors.append(f"missing_not_applicable_reason:{name}")
            continue
        receipts = layer.get("receipts")
        if not isinstance(receipts, list) or not receipts:
            errors.append(f"missing_receipts:{name}")
        elif not all(_receipt_succeeded(receipt, contract) for receipt in receipts):
            errors.append(f"unsuccessful_receipt:{name}")
        if name == "architecture":
            for field in ("owned_seam", "trust_boundary"):
                if not layer.get(field):
                    errors.append(f"missing_architecture:{field}")
            if not isinstance(layer.get("allowed_side_effects"), list):
                errors.append("missing_architecture:allowed_side_effects")
        elif name == "logic":
            methods = set(layer.get("methods") or [])
            if not methods or not methods.issubset({"unit", "property", "model", "oracle"}):
                errors.append("invalid_logic_methods")
        elif name == "component" and layer.get("uses_fakes"):
            if layer.get("fake_scope") != "external_edge_only":
                errors.append("component_fake_crosses_internal_seam")
        elif name == "adapter":
            if layer.get("mode") not in {"contract_fake", "live_canary"}:
                errors.append("adapter_mode_ambiguous")
        elif name == "e2e" and not layer.get("bounded_scope"):
            errors.append("missing_e2e_bounded_scope")
        elif name == "visual" and layer.get("primary_proof") is not False:
            errors.append("screenshot_cannot_be_primary_proof")
        elif name == "operational":
            for receipt in receipts or []:
                if not isinstance(receipt, dict) or receipt.get("exact_head") != contract.get("exact_head"):
                    errors.append("stale_operational_receipt")
        elif name == "negative_control" and layer.get("failure_detected") is not True:
            errors.append("negative_control_did_not_detect_failure")
    if all(layers.get(name, {}).get("status") == "not_applicable" for name in TEST_LAYERS):
        errors.append("no_executable_evidence_layer")
    return sorted(set(errors))


def closure_ready(contract: dict[str, Any]) -> bool:
    return not evidence_errors(contract)
