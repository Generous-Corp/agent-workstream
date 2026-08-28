---
name: workstream-resume
description: Use first whenever a request carries exactly one existing workstream handle such as GEN-37, a Linear issue URL, or a tab title and asks to inspect, resume, continue, execute, update, audit, or close it. Recover bounded authority before workstream-ledger or repository exploration.
---

# Workstream resume

Recover the authenticated, bounded current view before loading broader project
context. This skill is intentionally small so a stable handle remains a cheap
entry point.

After any mandatory host/session bootstrap that runs independently of this
request, the first functional command is:

```sh
python3 "<absolute directory of this SKILL.md>/scripts/workstream_resume.py" GEN-123
```

Only commands injected by the host independently of the model decision, plus
the mandatory exact read of this `SKILL.md`, count as bootstrap. No
model-selected cwd, environment, repository, memory, PATH, or lifecycle-skill
command is bootstrap.

Substitute the runtime-supplied directory containing this `SKILL.md` and the
single token from the request directly. If the request contains zero or
multiple distinct tokens, stop and ask for one; do not scout. Do not search for
the skill, inspect the environment, or execute the placeholder or an unset
variable.

Before repository, memory, worktree, PR, or plan exploration, run that command
exactly once. Do not probe `workstreamctl` on `PATH`. Never add
`--include-history` during initial recovery. The default helper validates full
history while returning bounded current authority.

If recovery refuses, report its exact error and stop. Do not fall back to local
context or load the lifecycle skill. Success requires `resume_authority` to be
`full`; inspection-only or unavailable authority is not permission to continue.

Report and retain the authenticated route/source, open children, current next
action, checkpoint, and attach-or-successor disposition. Do not claim live
repository or landing truth from the resume snapshot alone. For a status-only
request, report the bounded snapshot and stop; do not load `workstream-ledger`
or inspect repository, PR, or local state unless the user separately asks for
live reconciliation. If the request continues into execution, mutation,
checkpointing, audit, or closure, then load `workstream-ledger` and reconcile
only the named live surfaces.
