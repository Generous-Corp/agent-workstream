# Shipyard launch-profile bridge

`workstreamctl shipyard-profile` turns one current Agent Workstream checkpoint
into Shipyard's private `LaunchProfileV1` schema. It does not hand off a PR,
start an agent, write Linear, or install Shipyard.

## Create a profile

First persist a remotely acknowledged checkpoint for the exact current
material revision. The recorded worktree must be clean and `safe`, its GitHub
origin/head/branch/path must match the workstream scope, and Shipyard's active
`branch.<branch>.pulpWorktree{Status,DurableSha,LastPath}` lineage record must
match that checkout.

Create an owner-only directory and a new profile file:

```sh
install -d -m 700 ~/.local/share/agent-workstream/launch-profiles
plugins/workstream/bin/workstreamctl shipyard-profile GEN-123 \
  --repo-path /absolute/path/to/worktree \
  --model gpt-5.6-sol \
  --reasoning-effort medium \
  --output ~/.local/share/agent-workstream/launch-profiles/GEN-123.json
```

The command performs the normal authenticated full-authority Linear resume. A
private plan checkout may be supplied with `--plan-source`; `--config`,
`--plan-identity`, and `--linear-endpoint` have the same meaning as resume.
The output must live outside the bound worktree so creating it cannot invalidate
the clean-head proof or make private session data committable with the project.

Provider and exact session are derived from the latest checkpoint rather than
accepted as overrides. Model and effort are explicit because checkpoint schema
v1 does not record them. Codex and Claude argv are prompt-free and match
Shipyard's native grammar. Claude `ultra` effort is refused because Shipyard's
Claude adapter does not support it. When an ambient Codex or Claude session is
visible, it must exactly match that checkpoint. Without an ambient session,
pass the checkpoint's provider/session explicitly when creating Shipyard's
durable agent route.

On macOS, the output is created atomically with mode `0600` in an existing
owner-only directory and is never overwritten; any extended ACL on the parent,
temporary file, or result is also rejected. It contains a provider session ID
and an absolute worktree path, so keep it private and never commit it. Profile
publication currently fails closed outside macOS rather than claiming an ACL
guarantee it cannot prove.

After committing every required bump so the head cannot change, consume the
new file with the exact handle and `context_url` printed by the generator:

```sh
shipyard pr --no-apply-bumps \
  --workstream-id GEN-123 \
  --context-url https://linear.app/example/issue/GEN-123/example \
  --launch-profile ~/.local/share/agent-workstream/launch-profiles/GEN-123.json
```

The invoking ambient provider/session must match the checkpoint (or be supplied
explicitly through Shipyard's corresponding agent-route flags). See
[Shipyard's launch-profile contract](https://github.com/Generous-Corp/Shipyard/blob/main/docs/launch-profile.md)
for the lower-level steward-handoff form and trusted-consumer setup.

## Fail-closed gates

Profile creation refuses terminal, inspection-only, stale-lifecycle,
quarantined, incomplete, or over-budget resume state; a missing,
unacknowledged, unsafe, or non-current checkpoint; uncheckpointed material;
repository/head/worktree/disposition drift; a dirty or detached checkout; a
noncanonical GitHub origin; missing/stale lineage; unsupported provider/model/
effort; and an unsafe or existing output path.

## Digest contract

All bridge digests use:

```text
SHA-256(ASCII(domain) || NUL || canonical_json(value))
```

`canonical_json` is UTF-8 JSON with recursively sorted keys, Unicode preserved,
and no insignificant whitespace. For the resume-context digest only, the
set-like `children` surface is first sorted by case-insensitive stable issue
identifier; event lists retain their authoritative order.

- `checkpoint.digest` uses domain `agent-workstream-checkpoint-v1` and the
  compact latest-checkpoint object returned by bounded resume. Its generation
  is the recovered checkpoint provenance-chain count.
- `expected_resume_context_digest` uses domain
  `agent-workstream-resume-context-v1` and the complete bounded full-authority
  resume object.
- Success and failure use domain `agent-workstream-continuation-v1` over an
  object containing schema version, outcome, handle, checkpoint identity,
  checkpoint generation/digest, resume-context digest, repository, and head.

Any Linear or Git authority change therefore requires a new remotely
acknowledged checkpoint and a new profile. A fresh agent must reconstruct the
same bounded resume object and compare its digest before acknowledging
Shipyard's context challenge.
