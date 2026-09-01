# Reviewed native root transitions

Generation authority is append-only. A canonical plan URL in the Linear issue
description is only a freshness locator, and native Linear state is only the
status readback. Use this command solely for reviewed same-root mutations that
either prepare generation activation or reconcile a regressed locator to the
already-active structured generation. It never creates an issue, project, or
planning source.

Preview a same-document pinned-commit locator migration:

```sh
workstreamctl root-transition plan-url GEN-123 \
  --to https://github.com/owner/plans/blob/main/PLAN.md \
  --operator-contract generation-review.json \
  --plan-source ./PLAN.md \
  --plan-identity https://github.com/owner/plans/blob/main/PLAN.md
```

Preview reopening the exact terminal root to a reviewed Linear `started` state:

```sh
workstreamctl root-transition reopen GEN-123 \
  --state-id <linear-state-uuid> \
  --operator-contract generation-review.json \
  --plan-source ./PLAN.md \
  --plan-identity https://github.com/owner/plans/blob/main/PLAN.md
```

If ordinary resume reports `plan_generation_pending` because the description's
locator regressed while the structured active generation is still correct,
preview an in-place locator reconciliation instead of preparing that same
generation again:

```sh
workstreamctl root-transition reconcile-plan-url GEN-123 \
  --to https://github.com/owner/plans/blob/<40-hex-commit>/PLAN.md
```

This separate mode authenticates the immutable target bytes against the unique
structured active generation and exact active projection source. It refuses a
legacy-only or forked generation, a pending generation reservation, a source
mismatch, or a different plan document. It changes only the one labeled URL;
it creates no generation, projection, material, checkpoint, issue, project, or
plan artifact. A first real repair adds the same deterministic reservation used
by other root transitions; an already-correct fresh invocation and exact replay
are zero-write. Use generation preparation instead when the canonical bytes
intentionally represent a new plan—resume cannot infer that chronology.

The contract must be the exact `activation_ready` output from `workstreamctl
generation prepare`. It binds the target generation bytes/source, reviewed
started-state readback, authenticated route, and predecessor quiescence
frontiers. The command recomputes the complete prepare result from the same
stable graph/comment pair used for preview and again at the immediate prewrite
boundary, and requires exact equality; changing `phase` and recomputing the caller's
self-digest cannot authorize an incomplete candidate. A caller-selected state
UUID or unrelated plan source is refused. This candidate contract applies to
`plan-url` and `reopen`; `reconcile-plan-url` uses the active-generation
authorization described above and cannot substitute for activation review.

Review `intent`, `authenticated_route`, `expected_snapshot_sha256`,
`expected_frontier_sha256`, and `intent_sha256`, then repeat the identical
command with:

```sh
--expected-snapshot-sha256 <preview-value> \
--expected-frontier-sha256 <preview-value> \
--expected-intent-sha256 <preview-value> --apply
```

Apply reauthenticates the workspace, team, project, immutable root, and the
operation-specific authority: reviewed target state for activation preparation,
or the exact active generation and source for locator reconciliation. It then
claims a deterministic append-only reservation, rereads immediately before
`issueUpdate`, and validates exact route, state or description, and frontier
afterward. Repeating the exact apply is a zero-write recovery path.
Zero or multiple canonical URLs, a different document, stale review digests,
route drift, nonterminal reopen, a non-`started` target, or a competing intent
refuses without a mutable write.

Linear does not expose conditional `issueUpdate`. The reservation serializes
clients following this protocol, but cannot make the native write atomic
against unrelated writers. A contradictory post-read therefore fails closed
and requires human reconciliation; the command never silently retries or rolls
back mutable issue data.

Only the uninterrupted process that receives and verifies a newly created
reservation may call `issueUpdate`. An identical concurrent caller, an unknown
create outcome, or a restart that finds the root still in its pre-state refuses
as pending without a native write. Recovery is deliberate: inspect the pending
reservation, run a fresh preview whose frontier includes it, review the new
three digests, and apply that new intent. If the original native update already
landed, the old exact apply validates the target and returns a zero-write replay.
Reservation deletion, duplication, or body replacement at either the immediate
prewrite or final postread boundary refuses.
