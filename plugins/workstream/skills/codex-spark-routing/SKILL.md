---
name: codex-spark-routing
description: Route small, well-specified coding slices to GPT-5.3-Codex-Spark while a stronger owner retains architecture, integration, and final verification. Use when the user asks to accelerate work with Spark or when a bounded implementation/test slice would benefit from very fast iteration.
---

# Codex Spark Routing

Spend Spark on execution, not discovery. It has separate, limited preview capacity.

## Use Spark when

- the slice has a precise behavioral contract and deterministic targeted tests;
- ownership is narrow, normally one or two files with no concurrent writer;
- the change is mechanical or locally reasoned: parser hardening, fixtures, focused tests, small refactors, or documentation tied to code;
- a stronger owner will inspect the diff and retain responsibility for integration.

Do not use Spark as the final owner for architecture or authority boundaries, authentication, destructive operations, cross-machine rollout semantics, ambiguous failures, adversarial closure, or long-horizon autonomous work. Do not spend Spark on polling or routine status monitoring.

## Launch contract

Prefer an isolated worktree when another agent may edit overlapping files. Otherwise establish exclusive file ownership before launch.

The verified Codex CLI shape as of 2026-08-21 is:

```bash
codex exec \
  --ignore-user-config \
  --model gpt-5.3-codex-spark \
  --sandbox workspace-write \
  --cd /absolute/path/to/worktree \
  "<bounded implementation prompt>"
```

`--ignore-user-config` retains Codex authentication while avoiding unrelated
global plugins and hooks that can repeatedly inject large context into a small
worker run. Keep project instructions and execution rules enabled. Do not use
`--ephemeral`: the saved session is the recovery handle if preview capacity is
exhausted.

The prompt must name:

- the exact files Spark may modify;
- the single behavior to implement;
- adjacent concerns it must not address;
- the exact targeted test command it must run;
- whether committing or pushing is forbidden.

Spark does not run tests by default, so request them explicitly. Do not add unsupported `codex exec` flags from memory. If the command rejects an option, inspect `codex exec --help` once and correct the invocation; do not repeat model research.

## Acceptance

After Spark exits, the owning agent must inspect the actual diff, rerun the targeted tests, check for scope drift, and integrate or discard the result. A Spark report is not proof by itself. Spark should not commit or push unless the user explicitly assigns it ownership through landing.

Do not blindly retry a failed Spark run. Use its concrete failure to narrow one follow-up attempt, or finish the slice with the owning model when the failure reflects ambiguity rather than implementation mechanics.

## Capacity recovery

Record the session UUID printed at launch together with the worktree, allowed
files, last test result, and remaining action. If Spark reaches a capacity limit,
leave its worktree unchanged and resume the same session after capacity returns:

```bash
codex exec resume \
  --ignore-user-config \
  --model gpt-5.3-codex-spark \
  <session-uuid> \
  "Continue the same bounded slice from the current worktree. First inspect the existing diff; do not restart completed work."
```

If that session cannot be resumed, the stronger owning agent must inspect and
continue from the on-disk diff. Do not start a fresh Spark session merely to
reconstruct context already preserved in the worktree.
