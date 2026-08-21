---
name: decision-audit
description: Independently audit implementation choices that a specification left open. Use as a fresh-context, read-only closeout pass for substantial or high-risk work; do not use it as an editing or general code-review workflow.
---

# Decision audit

Audit the choices the implementer had to make because the accepted specification
was silent, ambiguous, or internally incomplete. This is an independent review,
not a rewrite pass.

## Independence and authority

- Start from the accepted specification, exact repository coordinates and Git
  heads, changed code, tests, and raw evidence. Do not rely on the implementer's
  persuasive summary or prior conversation when the source artifacts answer the
  question.
- Remain read-only. Never edit code, plans, issues, branches, PRs, or durable
  decision records. Return findings to the implementing agent or owner.
- Separate whether the choice is technically sound from whether it plausibly
  matches the owner's intent. Uncertainty in either dimension is material.

## Find and assess choices

Identify consequential decisions not dictated by the specification: ownership,
boundaries, data shape, failure behavior, ordering, retry policy, compatibility,
dependencies, reversibility, and omitted alternatives. Do not inflate ordinary
syntax or mechanical implementation details into choices.

For every material choice report:

- the specification gap, decision, and realistic alternatives;
- owning workstream child, plan revision, immutable provider repository key,
  canonical routing/display coordinate, and exact full Git object ID;
- reach: local, component, system, cross-system, or fleet;
- reversibility and affected domains;
- technical confidence and intent confidence, each as low, medium, or high;
- evidence inspected and verdict: `accepted`, `provisional`, or `must_fix`.

Rank findings by verdict first (`must_fix` first), then broader reach,
irreversibility, and lower confidence. Explain the concrete failure or lock-in,
not merely a preference.

Security, authority, persistence, concurrency, release, fleet, and irreversible
choices cannot be accepted provisionally. A `must_fix` verdict blocks landing
until a separate implementing pass fixes or supersedes it and the exact new head
is audited. Reversible, low-risk choices may remain provisional when the owner
can safely revisit them later; they still require an explicit audit verdict and
a stated trigger for review.

End with a landing verdict and a compact list of durable choice events the
implementer should record. Preserve earlier recorded/audited/superseded events;
never ask for history to be rewritten.
