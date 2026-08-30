# Material semantic repair manifest

Use this path only for a reviewed, digest-valid historical event whose
`material_boundary` payload is structurally invalid. The command previews by
default and appends exactly one control comment only with `--apply`.

The `--prepare` input is a reviewed seed, not a hand-assembled live frontier:

```json
{
  "schema_version": 1,
  "workstream_id": "GEN-37",
  "target_bindings": [{"...": "the two reviewed bindings"}],
  "authenticated_route": {"workspace_id": "...", "team_id": "...", "project_id": "...", "root_issue_id": "..."},
  "authenticated_source": {"identity": "...", "sha256": "..."},
  "generation": {"plan_revision": "...", "transition_tip_event_id": null, "activation_epoch": null, "authority_origin": "legacy_description"},
  "review_artifact": {"identity": "...", "repository": "...", "commit": "...", "path": "...", "sha256": "...", "reviewed_at": "..."},
  "strict_target_candidate_sha256": "..."
}
```

After review pins that seed and immutable artifact binding, generate every raw,
checkpoint, projection, graph, serialization, oracle, control identity/digest,
and the current deterministic remote slot from one authenticated zero-write
read, without hand calculation:

```sh
workstreamctl material-repair GEN-123 --manifest reviewed-payload.json \
  --review-artifact reviewed-targets.json \
  --plan-source ./PLAN.md --plan-identity <canonical-url> --prepare \
  > pinned-repair-manifest.json
```

Review and archive those exact output bytes. Then use the ordinary dry-run and
`--apply` commands with `pinned-repair-manifest.json`.

The reviewed JSON file has this exact outer shape:

```json
{
  "schema_version": 1,
  "control": {
    "kind": "material_semantic_repair",
    "source": "system",
    "event_id": "wsd_<deterministic sha256 prefix>",
    "expected_revision": 57,
    "created_at": "<reviewed UTC time>",
    "remote_slot_id": "<deterministic UUIDv4 boundary slot>",
    "payload_sha256": "<canonical payload sha256>",
    "canonical_event_sha256": "<v1 canonical event sha256>",
    "comment_body_sha256": "<encoded comment body sha256>"
  },
  "payload": {
    "schema_version": 1,
    "workstream_id": "GEN-37",
    "target_bindings": [
      {
        "event_id": "wsd_...",
        "remote_comment_id": "...",
        "comment_body_sha256": "...",
        "canonical_event_sha256": "...",
        "payload_sha256": "...",
        "original_expected_revision": 55,
        "original_index_zero_based": 55,
        "original_applied_revision": 56,
        "replacement": {
          "boundary_id": "repair:<target-event-id>",
          "changes": [{"kind": "progress", "payload": {}}]
        }
      }
    ],
    "raw_frontier": {
      "algorithm": "raw-reducer-order-v1",
      "revision": 57,
      "event_ids_reducer_order_sha256": "...",
      "events_sha256": "...",
      "remote_map_sha256": "..."
    },
    "checkpoint_frontier": {
      "algorithm": "checkpoint-reducer-order-v1",
      "count": 21,
      "revision": 53,
      "event_ids_reducer_order_sha256": "...",
      "event_ids_sorted_set_sha256": "...",
      "checkpoints_sha256": "..."
    },
    "projection_frontier": {
      "algorithm": "active-projection-reducer-order-v1",
      "revision": 82,
      "frontier_event_id": "wsp_...",
      "events_sha256": "..."
    },
    "generation": {
      "plan_revision": "...",
      "transition_tip_event_id": null,
      "activation_epoch": null,
      "authority_origin": "legacy_description"
    },
    "authenticated_route": {
      "workspace_id": "...", "team_id": "...", "project_id": "...",
      "root_issue_id": "..."
    },
    "authenticated_source": {"identity": "<immutable URL>", "sha256": "..."},
    "issue_graph_frontier": {
      "algorithm": "authenticated-root-children-relations-v1",
      "issues": {"root": {}, "children": [], "relations": [], "relation_targets": {}},
      "sha256": "..."
    },
    "ledger_serialization_frontier": ["<sorted checkpoint/reservation token>"],
    "postwrite_oracle": {
      "schema_version": 1,
      "target_binding_count": 2,
      "target_bindings_sha256": "...",
      "strict_target_candidate_sha256": "...",
      "source_identity": "...", "source_sha256": "...",
      "source_event_id": "wsp_...", "source_remote_comment_id": "...",
      "source_comment_body_sha256": "...", "source_event_sha256": "...",
      "projection_seal_event_id": "wsp_...",
      "projection_seal_remote_comment_id": "...",
      "projection_seal_comment_body_sha256": "...",
      "projection_seal_event_sha256": "...",
      "generation_tip_event_id": null,
      "fences_sha256": "..."
    },
    "review_artifact": {
      "identity": "<immutable commit URL to reviewed artifact>",
      "repository": "<immutable repository key>",
      "commit": "<40 hex>", "path": "<path>", "sha256": "<64 hex>",
      "reviewed_at": "<UTC time>"
    }
  }
}
```

All JSON digests use UTF-8 JSON with sorted keys and separators `,` and `:`.
Event ID uses `event_id_for(..., source="system")`; the remote slot uses the
shared ledger-boundary slot at the pre-repair raw revision and exact checkpoint
plus reservation frontier. The reducer-order checkpoint digest preserves the
checkpoint reducer's order. The separately labeled sorted-set digest prevents
an ordering convention from being mistaken for the frontier identity.

The reviewed target artifact is a separate exact JSON object containing only
`schema_version`, `workstream_id`, and the two target bindings. The CLI reads
those local bytes, verifies their SHA-256, verifies their content against the
payload, and checks that the immutable identity contains the bound commit and
path before any remote read or write.

The normalized replacement retains the target event ID, workstream, source,
expected revision, and creation time at its original zero-based index. The
only accepted transformation is boundary ID `repair:<target-event-id>` with
exactly one `progress` change whose payload canonically equals the entire
original flat payload. The control itself has no business semantics. Resume validates raw envelopes first,
then every repair binding/frontier, and only then evaluates effective next
action, blocker, obligations, and checkpoint state.

The deterministic material slot is one append-only write. Route, source,
checkpoint, projection, generation, graph, and serialization checks are a final
complete preflight plus postcheck, not a transactional cross-surface CAS; a
concurrent change is reported for reconciliation.

If apply returns `durable_partial_replay_required`, the control comment was
reobserved with its exact receipt. If it returns
`outcome_unknown_replay_required`, no receipt was observable after the failed
request. In either case, do not build another manifest or advance its revision. Rerun the exact
same command and bytes: the pinned event ID and remote slot take the receipt-only
replay path and repeat full post-read validation without another write.

Successful output deliberately reports production compact resume, full resume,
strict generation-candidate loading, and exact replay as `external_gate_required`
until those commands have actually been run against the deployed runtime.
