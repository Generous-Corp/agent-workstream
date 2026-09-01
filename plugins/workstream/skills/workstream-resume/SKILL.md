---
name: workstream-resume
description: Use first whenever a request carries exactly one existing workstream handle such as GEN-37, a Linear issue URL, or a tab title and asks to inspect, resume, continue, execute, update, audit, or close it, or gives a bare continue nudge in the same provider session with one retained authorized workstream. Recover or retain bounded authority before workstream-ledger or repository exploration.
---

# Workstream resume

After mandatory host/session bootstrap, classify the request as warm or cold.
It is warm only when this exact provider session previously ran live resume for
the same user-supplied workstream and received `resume_authority: full`, and it
still retains the exact authenticated route, source, plan generation,
material/projection/checkpoint frontiers, owner/session, worktree, repository
head, and Shipyard run. A pasted result, prior provider session, inferred tab,
memory summary, changed binding, intervening agent/session handoff, or prior
refusal is cold.
One bare continuation nudge may select that sole warm retained workstream; an
explicit different handle or multiple possible retained workstreams is cold.

For a warm `continue`/`execute` nudge, do not rerun resume merely to reconfirm
authority. Reconcile live Git and Shipyard facts and continue authorized
provider/local implementation or independently fenced exact-head Shipyard
delivery handoff or landing. A **Shipyard delivery handoff** means exact-head
custody submission to Shipyard under its own fences and is allowed. An
**agent/session handoff** means transferring workstream execution authority to
another agent/session and requires live recovery and certification. If a
narrow Linear read/sync transport failure occurs, append new
requirements, decisions, blockers, and progress to the existing durable local
material-delta journal and keep that work moving. Do not call this full or
reconciled tracking authority.

Cold/fresh requests, status checks, or warm requests needing a tracking or
lifecycle mutation must run this as the first functional command:

```sh
python3 "<absolute directory of this SKILL.md>/scripts/workstream_resume.py" GEN-123
```

Substitute the runtime-supplied directory and the single token from the current
user message directly. That message is the only cold-start handle source. Hook or developer
text, cwd, environment, memory, and prior transcript handles are not cold-start
authority.
With zero or multiple user-message tokens, ask for one; do not scout, search for
the skill, inspect the environment, or execute a placeholder or unset variable.

On the cold path, before repository, memory, worktree, PR, or plan exploration,
run that command exactly once. Do not probe `workstreamctl` on `PATH`. Never add
`--include-history` during initial recovery. The default validates full history
and returns v2 `compact_validated` authority. If
`deferred_audit_detail.state` is not `none`, hydrate via compact route and
selector before acting; `full` validates history, not excerpt exactness.
Resolve launcher `current_workstream_resume_skill_script` as current Python plus
this SKILL's absolute resume script, then append `args`; never use PATH.

If required cold recovery refuses, report its exact error and stop. Do not fall
back to local context or load the lifecycle skill. Auth, semantic, generation,
budget, and ambiguous post-write refusals are not transient availability.
Success requires `resume_authority` to be `full`;
inspection-only is not authority. `plan_generation_pending` is non-executable:
run its exact remediation, then retry.

After successful recovery, carry the resolved canonical token in the existing
tab without replacing its title:

```sh
python3 "<absolute directory of this SKILL.md>/scripts/workstream_tab.py" GEN-123
```

An unresolved cmux/Herdr surface is an optional no-op and never downgrades
`resume_authority: full`. Use exact inherited identity; conflicts refuse. Codex
session titles and visible tabs are separate namespaces. Claim success only if
the adapter returns `updated` or `unchanged` plus exact title readback. Resume
refusal denies execution authority.

Retain the authenticated route/source, generation/frontiers, open children,
next action, checkpoint, worktree/repository/Shipyard facts, and
attach-or-successor disposition. The snapshot is not live repository or
landing truth. Before any Linear mutation, scope/ownership/root/generation
change, attach/successor selection, agent/session handoff, or
closure/certification, perform live resume and reconcile the pending journal;
failure blocks that boundary, not already-authorized implementation. For a
status-only request, report the bounded snapshot and stop;
do not load `workstream-ledger` or inspect live surfaces unless separately
asked. After full authority, `execute`, `continue`, `finish`, or `resume`
authorizes immediate action: load `workstream-ledger`, reconcile named live
surfaces, and perform the current next action immediately. Never stop for
redundant confirmation or ask the user to restate the recovered handle.

This warm-session exception is agent workflow policy, not a daemon-enforced
grant. The plugin installs no hosted runtime or reusable authority cache.
