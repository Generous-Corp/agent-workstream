# Use Agent Workstream

## 1. Install and authenticate

Install the plugin using [INSTALL.md](INSTALL.md), complete the short
[Linear setup](LINEAR_SETUP.md), then start a new agent session. Supply
`LINEAR_API_KEY` through your shell or secret manager when live Linear reads or
writes are required. Never commit it.

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

Useful follow-ups are:

```text
What changed in GEN-123 since its last checkpoint?
Record this new requirement in GEN-123 and continue.
Adversarially check whether GEN-123 can close.
```

The agent records material changes only. Diagnostic conversation with no scope,
decision, blocker, evidence, or next-action change should produce no ledger
write.

## Deterministic helpers

From a checkout of this repository:

```sh
# Snapshot a plan without writing to Linear.
plugins/workstream/bin/workstreamctl plan ./PLAN.md \
  --identity https://github.com/example/plans/blob/main/PLAN.md

# Resolve and validate one live Linear root and its nonterminal children.
plugins/workstream/bin/workstreamctl resume GEN-123
```

The plugin is not a hosted orchestration service. It installs no hooks,
monitors, MCP servers, or background workers. Optional ingress and landing
adapters must be configured separately, and unavailable live surfaces remain
explicit rather than being inferred from a checkout or transcript.
