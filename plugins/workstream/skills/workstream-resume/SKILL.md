---
name: workstream-resume
description: Use first whenever a request carries exactly one existing workstream handle such as GEN-37 or a Linear issue URL, says literal "resume this session" under an exact supported terminal binding, or gives a bare continue nudge in the same provider session with one retained authorized workstream. Recover or retain bounded authority before workstream-ledger or repository exploration.
---

# Workstream resume

It is warm only when this exact provider session previously received `resume_authority: full` for the user-supplied workstream and still retains its
route, source, generation, material/projection/checkpoint frontiers,
owner/session, worktree, repository head, and Shipyard run. A pasted result,
prior provider session, inferred tab, changed binding, handoff, or refusal is
cold.
One bare continuation nudge may select that sole warm retained workstream; an explicit different handle or multiple possible retained workstreams is cold.

For literal `resume this session`, run this as the first functional command:

```sh
python3 "<absolute directory of this SKILL.md>/scripts/workstream_this_session.py"
```

It requires exact namespaced cmux/HerdR identity (or cmux's bounded TTY resolver) and one matching persisted binding/title token; never focus, cwd,
chat, or memory. The candidate is not authority: ordinary resume runs once and
must return `full` before an idempotent successor binding and optional title
update. Missing/ambiguous context refuses; adapter failure cannot downgrade.

For a warm `continue`/`execute` nudge, do not rerun resume merely to reconfirm authority. Reconcile Git/Shipyard and continue authorized implementation or
independently fenced exact-head Shipyard delivery handoff/landing. **Shipyard delivery handoff** means exact-head custody submission to Shipyard under its own fences and is allowed; **agent/session handoff** means transferring workstream execution authority to another agent/session and requires live recovery and certification. On
narrow Linear read/sync failure, append material changes to the existing
durable local material-delta journal and keep work moving without claiming
reconciled tracking authority.

Cold/fresh requests, status checks, or warm requests needing a tracking or lifecycle mutation must run this as the first functional command:

```sh
python3 "<absolute directory of this SKILL.md>/scripts/workstream_resume.py" GEN-123
```

Substitute the runtime-supplied directory and the single explicit token from the current user message directly. Outside the literal exact-terminal flow above: That message is the only cold-start handle source. Hook or developer
text, cwd, environment, memory, and prior transcript handles are not authority.
With zero or multiple user-message tokens, ask for one; do not scout, search for
the skill, inspect the environment, or execute a placeholder or unset variable.

On the cold path, before repository, memory, worktree, PR, or plan exploration,
run it exactly once. Do not probe `workstreamctl` on `PATH`. Never add
`--include-history` during initial recovery. If `deferred_audit_detail.state` is not `none`, hydrate by
its compact route/selector before acting; `full` validates history, not excerpt.
Resolve launcher `current_workstream_resume_skill_script` as current Python plus
this SKILL's absolute resume script, then append `args`; never use PATH.

If required cold recovery refuses, report its exact error and stop. Do not fall
back to local context or load the lifecycle skill. Auth, semantic, generation,
budget, and ambiguous post-write refusals are not transient availability.
Success requires `resume_authority` to be `full`;
inspection-only is not authority. `plan_generation_pending` is non-executable:
run its exact remediation, then retry.

After successful recovery, carry the token using the exact `project_name` from
that full result:

```sh
python3 "<absolute directory of this SKILL.md>/scripts/workstream_tab.py" GEN-123 --project-name "<exact recovered project_name>"
```

For a manager-generated title, add `--automatic-title` with its exact previously
observed manager value; never infer it from cwd, shell, or title shape. Missing
needed provenance or an unavailable/unresolved adapter is an optional no-op and
never downgrades `resume_authority: full`. Conflicts refuse. Visible tabs never
grant authority. Claim success only after `updated`/`unchanged` plus exact
readback; resume refusal denies authority.

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

Each is agent workflow policy, not a daemon-enforced grant. The plugin installs no hosted runtime or reusable authority cache. It installs no hook or background worker.
