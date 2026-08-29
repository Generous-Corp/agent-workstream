# Optional durable prompt ingress

Read this only when a caller has explicitly chosen to operate the ingress
transport. Normal Codex or Claude plugin installation does not enable capture,
install a hook, start a process, or create a remote issue.

## Boundary

Ingress is an optional at-least-once safety net for the interval between a user
turn and its promotion into the durable Linear graph. It is not a transcript
authority or task tracker:

1. An external, trusted capture integration invokes `capture` with a Codex or
   Claude prompt-event payload.
2. `capture` first inserts one idempotent event into a mode-0600 local SQLite
   outbox, then attempts a bounded upload to a configured private GitHub issue.
3. `recover` reads remote capture, bind, promotion-intent, and processed markers
   and deduplicates them by `event_id`.
4. A successor stages one reviewed, bounded promotion intent in that private
   stream, applies its deterministic material event to Linear, verifies the
   Linear receipt, and only then appends the processed successor marker.

No capture integration ships in v1 because a plugin-cache path is not a stable
launcher path. A deployment that wants ingress must supply and own a stable
launcher outside the plugin cache. Until then, cross-machine recovery of turns
that arrived after the last remote acknowledgement is unavailable.

Resolve the directory containing the skill's `SKILL.md` as
`WORKSTREAM_SKILL_ROOT`. Operating ingress is explicit:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" configure \
  --repo your-org/private-workstream-ingress --machine build-mac
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" status
```

`configure` is a remote mutation: it creates or reuses a private, monthly
GitHub issue and writes its coordinates to the local mode-0600 config. `capture`
writes the local outbox and may append a remote comment. `bind`, `unbind`, and
`process` may append remote comments. `flush` retries pending uploads. None of
these commands runs during ordinary plugin installation.

## Identity and recovery

Capture may receive generic `WORKSTREAM_ID`, `WORKSTREAM_SURFACE_ID`, and
`WORKSTREAM_WORKSPACE_ID` environment values. Provider session IDs and explicit
`--event`, `--session`, or `--surface` values are also accepted. Compatibility
inputs such as cmux surface variables may be used by an optional adapter, but
cwd is never a workstream identity. Herdr public IDs are scoped to one socket:
an adapter must bind `HERDR_PANE_ID`/`HERDR_WORKSPACE_ID` to a digest of the
inherited `HERDR_SOCKET_PATH` and must never persist a bare `w1:p1` or `w1` as a
cross-session identity.

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" flush
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" recover \
  --workstream ABC-123
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" promote \
  --request reviewed-promotion.json --apply
```

An unprocessed event is an open triage obligation, not evidence that its request
was accepted. Binding fails closed without an exact event, provider session, or
trusted adapter surface. Correct a mistaken binding with `unbind`; never bind by
repository cwd.

The reviewed promotion request is exact JSON:

```json
{
  "schema_version": 1,
  "ingress": {
    "repo": "your-org/private-workstream-ingress",
    "remote_issue": 42,
    "event_id": "wsi_...",
    "prompt_sha256": "<64 lowercase hex>"
  },
  "authority": {
    "workspace_id": "<Linear workspace UUID>",
    "team_id": "<Linear team UUID>",
    "project_id": "<Linear project UUID>",
    "root_issue_id": "<immutable Linear root issue UUID>"
  },
  "workstream_id": "ABC-123",
  "expected_material_revision": 7,
  "changes": [
    {"kind": "requirement", "payload": {"text": "...", "acceptance": "..."}}
  ]
}
```

`promote` does not classify prompt text. Without `--apply` it is a zero-write
preview. With `--apply`, it first appends the immutable promotion intent. If the
agent or source machine then disappears, a successor needs only the private
repo, issue, and event identifiers:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" promote \
  --repo your-org/private-workstream-ingress --remote-issue 42 \
  --event wsi_... --apply
```

Replay reuses one deterministic Linear material-event ID. A crash after the
Linear mutation but before the processed marker therefore cannot duplicate the
work, and a crash after the processed marker is a zero-write replay. GitHub does
not offer client-supplied comment IDs, so simultaneous identical writers may
append duplicate physical comments; the authenticated reducer treats them as
one logical marker and refuses conflicting copies.

The promotion identity includes the canonical lowercase GitHub `owner/repo`
and exact ingress issue number as well as the raw event and immutable Linear
route. Route-bearing promotion markers use schema version 2; version 1 markers
are refused because they did not carry that identity. Replaying the same-looking
event from another repository or issue refuses. Both the reviewed request and the final encoded promotion/processed
comment envelopes are capped at 16 KiB; expansion during encoding is checked
before any write. Recovery does not hide a promoted capture until read-only
Linear validation proves the marker's exact event and receipt.

Use the older `process` command only for reviewed `no-material-delta` or
`superseded` dispositions. Material promotion must use `promote`, which proves
the Linear receipt before acknowledging the raw event. `configure` records the
currently authenticated GitHub actor as the non-material classification trust
boundary. Recovery accepts a classification only when the authenticated API
metadata says that exact actor posted it and its schema-2 payload binds the
physical repo/issue route, raw capture digest, event, source, disposition, and
deterministic classification receipt. A well-formed marker posted by any other
actor remains open and causes recovery to refuse; body shape alone is never
authority. Changing the trusted actor requires rerunning `configure` deliberately.

## Privacy and retention

Ingress stores prompt content, so use only a private repository and do not paste
secrets into prompts. Before local or remote persistence, the current sanitizer:

- removes userinfo from any hierarchical `scheme://user:password@host` URL;
- redacts values for the URL query keys `code`, `token`, `access_token`,
  `refresh_token`, `id_token`, `state`, `client_secret`, `key`, and `password`;
- redacts bearer headers, common secret/password assignments, supported token
  prefixes, and PEM private-key or certificate blocks; and
- applies the same sanitizer to persisted and returned failure details.

This is bounded pattern matching, not a proof that arbitrary secret formats are
undetectable. The original prompt SHA-256 remains in the event for identity.
Prompt bodies are capped at 16 KiB. Locally acknowledged rows older than 30
days may be pruned; unuploaded rows are retained. Remote deletion is not
automatic.

Authentication uses an existing `GH_TOKEN` or `GITHUB_TOKEN`, or a mode-0600
file at `~/.config/workstream/ingress-token`. Override the latter with
`WORKSTREAM_INGRESS_TOKEN_FILE`. The repository has no personal credential or
remote-repository default.

## Optional adapters

Session managers may provide stable surface identity and continuation UX.
Landing controllers may provide exact-head ownership and receipts. cmux, Herdr,
and Shipyard are examples; none is required or an authority for the Linear
workstream graph. Herdr is detected only through its explicit inherited
environment, never by probing a focused/default session.
