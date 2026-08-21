# Agent Workstream

Agent Workstream makes long-running agent work durable instead of leaving its
truth inside one chat, checkout, or machine.

Its goals are to:

- start a project once and resume it later from one stable handle;
- evolve scope without erasing what changed or why;
- retain the plan, repositories, worktrees, machines, status, decisions, and
  exact-head evidence needed by the next agent;
- isolate projects and repository identities while linking real dependencies;
- refuse closure when work, evidence, ownership, or acceptance is incomplete.

## How it works

Linear is the pilot durable ledger. Deterministic Python reducers validate plan
intake, scope, deltas, checkpoints, evidence, resume snapshots, and closure.
The [workstream-ledger skill](plugins/workstream/skills/workstream-ledger/SKILL.md)
guides normal execution; [decision-audit](plugins/workstream/skills/decision-audit/SKILL.md)
provides a fresh-context, read-only challenge to choices the specification left
open. One plugin payload serves both Codex and Claude Code.

Examples:

```text
Start a tracked workstream for this plan.
What changed in ABC-123 since the last checkpoint?
Resume ABC-123 and reconcile it with live repository state.
Adversarially check whether ABC-123 can close.
```

## Current boundary

The local contracts and tested Linear transports are usable, but this is not a
hosted orchestration service. Some resume surfaces and live cross-machine proof
remain explicitly unimplemented and therefore block closure. The plugin does
not install hooks, MCP servers, monitors, or background workers. Shipyard and
other delivery systems remain optional external adapters, not owners of the
workstream.

See [INSTALL.md](INSTALL.md) for installation and configuration.

This repository is intended to remain private. No public license is granted.
