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
Reviewed first intake uses route-scoped deterministic Linear issue IDs, so
concurrent creators converge on one exact root/child graph or fail closed.
An optional repository-root `.workstream.json` is the fail-closed routing
authority for its Linear workspace, team, and project; explicit overrides must
match it.
Ordinary authenticated resume remains bounded to 24 KiB after validating the
complete immutable history. If verbose current state would exceed that bound,
the response keeps a digest-bound execution frontier and exposes exact JSON
Pointers plus a full-history audit command in `deferred_audit_detail`; byte
growth alone does not strand a valid workstream. A checkpoint can fence an
acknowledged material-event prefix, but cannot by itself bound open-child
state, current decisions, dependencies, proposals, or projection authority.
The small [workstream-resume skill](plugins/workstream/skills/workstream-resume/SKILL.md)
recovers an existing handle before loading broader lifecycle guidance. The
[workstream-ledger skill](plugins/workstream/skills/workstream-ledger/SKILL.md)
guides execution and closure; [decision-audit](plugins/workstream/skills/decision-audit/SKILL.md)
provides a fresh-context, read-only challenge to choices the specification left
open. [codex-spark-routing](plugins/workstream/skills/codex-spark-routing/SKILL.md)
records when and how to spend limited Spark capacity on bounded implementation
slices, including session recovery. One plugin payload serves both Codex and
Claude Code.

## Possible today

Give an agent a Markdown plan, file path, or durable URL and ask it to start a
tracked workstream. From then on, the stable Linear token can be used to:

- update scope, tasks, dependencies, decisions, and blockers without losing
  their history;
- ask for current status or what changed since the last checkpoint;
- checkpoint the exact machine, repository, worktree, branch, head, evidence,
  and next action needed for continuation;
- generate a private exact-checkpoint Shipyard launch profile on macOS for an
  explicitly configured landing handoff;
- resume in another session, agent, or machine after reconciling Linear with
  live repository and landing state; and
- challenge completion adversarially so missing work or proof stays visible.

Typical requests are:

```text
Start a tracked workstream for this plan.
What changed in ABC-123 since the last checkpoint?
Resume ABC-123 and reconcile it with live repository state.
Adversarially check whether ABC-123 can close.
```

[cmux](https://cmux.com/) and [Herdr](https://herdr.dev/docs/) are optional.
When either manages the current tab, Agent Workstream can carry the stable token
in its title. Without a supported session manager, resume from the same Linear
token or URL; only the display convenience is skipped.

## Roadmap

The next layers under consideration are an optional conversation plane for
actionable Discord notifications and status requests, managed prompt ingress,
an optional hosted or self-hosted control plane, provider portability, exports,
and broader fleet automation. These are intentionally separate from the
portable ledger contract; see the dedicated [future roadmap](FUTURE.md).

## Current boundary

The local contracts and tested Linear transports are usable today. Checkpoint
resume has been exercised across machines, but complete physical recovery of
every authority surface and semantic closure still block final pilot closure.
The plugin intentionally remains a thin, local-first layer; optional always-on
coordination is planned as a separate companion. See the concise
[architecture boundary](BOUNDARIES.md) for what that enables and prevents.

See [INSTALL.md](INSTALL.md) for installation and configuration.
See [LINEAR_SETUP.md](LINEAR_SETUP.md) for the one-time Linear route and
authentication setup.
See [USAGE.md](USAGE.md) for the start, resume, status, and closure workflow.
See the [Shipyard profile bridge](plugins/workstream/skills/workstream-ledger/references/shipyard-launch-profile.md)
for the optional exact-checkpoint handoff contract.
See [BOUNDARIES.md](BOUNDARIES.md) for the local-first architecture boundary.
See [FUTURE.md](FUTURE.md) for the roadmap of deferred optional integrations.

Licensed under the [MIT License](LICENSE).
