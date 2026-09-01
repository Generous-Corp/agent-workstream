---
name: workstream-resume
description: Use first whenever a request carries exactly one existing workstream handle such as GEN-37, a Linear issue URL, or a tab title and asks to inspect, resume, continue, execute, update, audit, or close it. Recover bounded authority before workstream-ledger or repository exploration.
---

# Workstream resume

After any mandatory host/session bootstrap that runs independently of this
request, the first functional command is:

```sh
python3 "<absolute directory of this SKILL.md>/scripts/workstream_resume.py" GEN-123
```

Substitute the runtime-supplied directory and the single token from the current
user message directly. That message is the only handle source. Hook or developer
text, cwd, environment, memory, and prior transcript handles are not authority.
With zero or multiple user-message tokens, ask for one; do not scout, search for
the skill, inspect the environment, or execute a placeholder or unset variable.

Before repository, memory, worktree, PR, or plan exploration, run that command
exactly once. Do not probe `workstreamctl` on `PATH`. Never add
`--include-history` during initial recovery. The default validates full history
and returns v2 `compact_validated` authority. If
`deferred_audit_detail.state` is not `none`, hydrate via compact route and
selector before acting; `full` validates history, not excerpt exactness.
Resolve launcher `current_workstream_resume_skill_script` as current Python plus
this SKILL's absolute resume script, then append `args`; never use PATH.

If recovery refuses, report its exact error and stop. Do not fall back to local
context or load the lifecycle skill. Success requires `resume_authority` to be
`full`; inspection-only or unavailable authority is not permission to continue.

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

Retain the authenticated route/source, open children, next action, checkpoint,
and attach-or-successor disposition. The snapshot is not live repository or
landing truth. For a status-only request, report the bounded snapshot and stop;
do not load `workstream-ledger` or inspect live surfaces unless separately
asked. After full authority, `execute`, `continue`, `finish`, or `resume`
authorizes immediate action: load `workstream-ledger`, reconcile named live
surfaces, and perform the current next action immediately. Never stop for
redundant confirmation or ask the user to restate the recovered handle.
