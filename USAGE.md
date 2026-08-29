# Use Agent Workstream

## 1. Install and authenticate

Install the plugin using [INSTALL.md](INSTALL.md), complete the short
[Linear setup](LINEAR_SETUP.md), then start a new agent session. For seamless
unattended access, install the token once at
`~/.config/agent-workstream/linear.token`; environment and alternate protected
file options are also supported. Never commit it.

Optionally copy [the example declaration](examples/workstream.json) to
`.workstream.json` and replace its route and repository identifiers. Validate
it with:

```sh
plugins/workstream/bin/workstreamctl config validate .workstream.json
```

The repository-root declaration is consumed automatically for live Linear
routing. An explicit `--config` or `WORKSTREAM_CONFIG` may select a declaration
elsewhere; explicit route arguments must match it.

## 2. Start once

In Codex, say:

```text
Start a tracked workstream for ./PLAN.md. Use
https://github.com/example/plans/blob/main/PLAN.md as the canonical plan identity.
```

In Claude Code, use the same request after invoking:

```text
/workstream:workstream-ledger
```

The agent reviews the proposed child graph before writing it and returns one
stable Linear root token and URL. Keep that token; it is the resume handle.

## 3. Continue with the token

Any new Codex or Claude session can receive:

```text
Resume GEN-123. Reconcile the durable graph with live repository and landing
state before editing, then continue the recorded next action.
```

If you use [cmux](https://cmux.com/), the token can also be carried in a tab
title and passed into a successor session. This is optional integration: without
cmux, paste the same token or Linear URL into any new agent or terminal. Durable
state remains in Linear in either case. See the [cmux API](https://cmux.com/docs/api)
for its tab and workspace automation surface.

The agent runs `workstreamctl tab-title GEN-123` after intake or resume. It
appends the token to an existing human-readable title, does nothing when that
token is already present, and refuses to overwrite a different workstream
token. Outside cmux, the command exits successfully without changing anything.

Useful follow-ups are:

```text
What changed in GEN-123 since its last checkpoint?
Record this new requirement in GEN-123 and continue.
Adversarially check whether GEN-123 can close.
```

The agent records material changes only. Diagnostic conversation with no scope,
decision, blocker, evidence, or next-action change should produce no ledger
write.

## Common scenarios

| You want to | Ask the agent to |
| --- | --- |
| Start from a plan | `Start a tracked workstream for ./PLAN.md.` |
| Start from a durable plan URL | `Start a tracked workstream for <URL>.` |
| Resume elsewhere | `Resume GEN-123 and reconcile live state before continuing.` |
| Change the work | `Record this new requirement in GEN-123 and continue.` |
| Inspect progress | `Show status, blockers, and changes since the last checkpoint for GEN-123.` |
| Hand off safely | `Checkpoint GEN-123 with exact location, head, evidence, and next action.` |
| Attempt completion | `Adversarially verify whether GEN-123 can close.` |

The Linear URL may be used wherever the stable token is accepted. Starting from
a local Markdown file is supported, but provide a durable plan identity when
other machines must be able to retrieve the original artifact.

### Continue with the token

For a handle/URL/tab-title resume request, with or without additional
instructions, the agent's first command is:

```sh
python3 "<absolute directory of the SKILL.md loaded for this turn>/scripts/workstream_resume.py" GEN-123
```

The agent substitutes the runtime-supplied loaded skill path directly; it does
not search the filesystem, inspect cwd/environment, probe `PATH`, or execute the
placeholder. Initial recovery always omits `--include-history` and runs before
memory, worktree, repository, or PR inspection. A second full-history call is
reserved for the point when an explicitly requested audit or closure pass
actually begins.

## Deterministic helpers

From a checkout of this repository:

```sh
# Snapshot a plan without writing to Linear.
plugins/workstream/bin/workstreamctl plan ./PLAN.md \
  --identity https://github.com/example/plans/blob/<40-hex-commit>/PLAN.md

# After reviewing that preview, create exactly the accepted candidates.
plugins/workstream/bin/workstreamctl intake ./PLAN.md \
  --identity https://github.com/example/plans/blob/<40-hex-commit>/PLAN.md \
  --plan-revision copy-source-sha256-from-preview \
  --root-stable-key copy-root-stable-key-from-preview \
  --accept-key section-copy-from-preview \
  --accept-key section-copy-another-key

# Resolve one live root with full authority, fetching its projected plan bytes.
plugins/workstream/bin/workstreamctl resume GEN-123

# For a private checkout, override only the fetch location.
plugins/workstream/bin/workstreamctl resume GEN-123 \
  --plan-source ./PLAN.md

# Inspect an older root without claiming authority to continue it.
plugins/workstream/bin/workstreamctl resume GEN-123 --inspection-only

# Apply a reviewed repository rename/transfer request. The JSON names the exact
# immutable repository ID and material/projection/scope frontiers.
plugins/workstream/bin/workstreamctl repository-identity \
  --request repository-identity.json --apply

# After a current remote checkpoint, create one private Shipyard launch profile.
install -d -m 700 ~/.local/share/agent-workstream/launch-profiles
plugins/workstream/bin/workstreamctl shipyard-profile GEN-123 \
  --repo-path /absolute/path/to/worktree \
  --model gpt-5.6-sol --reasoning-effort medium \
  --output ~/.local/share/agent-workstream/launch-profiles/GEN-123.json
```

Those are secondary repository-local CLI examples; installation does not add
`workstreamctl` to global `PATH`.

`shipyard-profile` derives provider and session from the latest acknowledged
checkpoint, performs a fresh authenticated full-authority resume, validates the
exact clean GitHub worktree and active Shipyard lineage, and writes a new
owner-only file atomically on macOS. It refuses other platforms, stale or
uncheckpointed state, and never puts Linear credentials in the profile. See the
[launch-profile bridge contract](plugins/workstream/skills/workstream-ledger/references/shipyard-launch-profile.md).

Token-only resume tries HTTPS first. If an immutable GitHub blob URL at an
exact 40-hex commit returns 404, it can use existing noninteractive GitHub SSH
access in a temporary isolated repository. Mutable refs and failed or timed-out
SSH retrieval refuse authority; no API token is forwarded to the Git process.
The fallback requires POSIX process-group cleanup; HTTPS remains portable.

`intake` requires an authenticated, complete workspace/team/project route and
an explicit review (`--accept-key` for each child, or `--accept-none`). It
also requires the preview's exact source SHA-256 and root stable key, so changed
bytes or source identity force a new review, and returns exact root and child
receipts. Concurrent identical intake calls use
the same deterministic issue IDs and converge only after full readback; an ID
or field collision fails closed. A later call may add a newly reviewed missing
child from the same plan revision, but changed-plan and other update paths still
refuse without remote CAS.

Live resume also reduces the root's complete append-only material-event and
checkpoint history. It uses the latest durable event next action and, when one
exists, returns the acknowledged checkpoint's machine, worktree, exact head,
evidence, blocker, and provenance chain. Unimplemented surfaces remain named in
`surface_availability` instead of being inferred from local session state.
Normal resume validates the complete history, preserves current child details
and exact uncheckpointed requirements/blockers/decisions/follow-ups. It keeps
actionable checkpoint evidence while binding evidence, provenance, validated
routing detail, and older history with counts/digests. Add `--include-history`
for an audit or closure pass; explicit
byte/item caps still fail loudly rather than truncating required current state.
Full-authority output requires the append-only scope, source, provenance, and
attach/successor disposition projection. A reviewed projection manifest can be
applied idempotently with `workstreamctl projection GEN-123 manifest.json
--remote-head <40-or-64-hex-head> --plan-source ./PLAN.md --plan-identity <URL>`;
the manifest fences the exact current projection revision and active
key/event/value-digest set. Retirement is explicit and names the reviewed head;
omission never deletes live state. A stale review refuses before writing. A
positional JSON snapshot to `workstreamctl resume` is always inspection-only
and therefore requires `--inspection-only`; only a live authenticated Linear
read can produce full-authority output. The command writes only reviewed
changes and verifies a complete live readback.

After a PR lands, `workstreamctl reconcile --help` exposes the explicit GitHub,
Shipyard fixed-argv, plan, and closure-input arguments. The command records
`Landed — acceptance review required` from exact live truth. Supplying a
durably projected fresh-session review receipt bound to that exact snapshot is
the only path to a durable `Done`; a stale writer or unchanged replay writes
nothing. Use `--github-token-command` with a noninteractive file/keychain/App
helper, or explicitly opt into `--github-token-env GITHUB_TOKEN`; helper output
is bounded and never logged.

For one repository, the existing `--repository`, `--repository-id`, `--pr`, and
`--expected-head` flags remain supported. For a root spanning repositories,
repeat one qualified group per repository. The repository portion of the
command is:

```sh
--repository-binding '{"repository":"Generous-Corp/pulp","repository_id":"R_pulp","pr":123,"expected_head":"<40-hex>"}' \
--repository-binding '{"repository":"Generous-Corp/vellum","repository_id":"R_vellum","pr":456,"expected_head":"<40-hex>"}'
```

The fixed-argv receipt adapter returns one schema-v2 aggregate containing a
repository-qualified receipt for every group. Missing, duplicate, or drifted
repository truth blocks the aggregate lifecycle write.

Review receipts name and digest the reviewer’s durable artifact and declare the
`shared_linear_credential` trust boundary. This enforces a separate procedural
pass, not cryptographic agent identity. Linear CAS slots include the immutable
workspace/project/root route. A 2026-08-28 live canary verified that
`CommentCreateInput.id` is accepted, exact-ID replay collides, IDs are global
across issues, and cleanup succeeded; runtime schema introspection still fails
closed if that input disappears. See
[the bounded proof receipt](docs/reconcile-linear-cas-proof.md).
Late v1 writes remain visible as a bounded quarantine summary (and exact events
with `--include-history`) and block lifecycle updates until a reviewed durable
disposition names their exact IDs and digest.

Unavailable live surfaces remain explicit rather than being inferred from a
checkout or transcript. See [BOUNDARIES.md](BOUNDARIES.md) for why the plugin
does not silently install always-on orchestration and what optional companion
layers are planned.
