# Repository identity history seal

Use this only when resume reports `partial_reconcile_required` for a bounded set
of legacy repository-identity transitions. The partial result has no execution
authority.

Build a request from that exact result without changing any digest or revision:

```json
{
  "schema_version": 1,
  "workstream_id": "GEN-123",
  "authority": {
    "workspace_id": "...",
    "team_id": "...",
    "project_id": "...",
    "root_issue_id": "..."
  },
  "plan_revision": "<partial plan_revision>",
  "plan_source": "<partial source.identity>",
  "observed_at": "<review timestamp in RFC 3339 form>",
  "expected_frontier": {
    "material_revision": 0,
    "projection_revision": 0,
    "sealed_scope_event_id": "wsp_...",
    "sealed_scope_value_sha256": "...",
    "legacy_transitions": [{
      "predecessor_scope_event_id": "wsp_...",
      "predecessor_scope_value_sha256": "...",
      "transition_scope_event_id": "wsp_...",
      "transition_scope_value_sha256": "..."
    }],
    "sealed_projection_frontier_event_id": "wsp_...",
    "sealed_projection_frontier_event_sha256": "...",
    "legacy_projection_prefix_sha256": "..."
  }
}
```

Preview first; exit 3 means review is still required and performs no write:

```sh
workstreamctl repository-identity-seal --request request.json
```

The preview contains the full deterministic event and fresh provider receipts.
After review, apply the same request:

```sh
workstreamctl repository-identity-seal --request request.json --apply
```

Then run normal resume again. Only strict replay of the sealed history may
restore `resume_authority: full`.
