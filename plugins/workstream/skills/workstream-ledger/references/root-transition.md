# Reviewed native root transitions

Generation authority is append-only. A canonical plan URL in the Linear issue
description is only a freshness locator, and native Linear state is only the
status readback. Use this command solely for the reviewed same-root mutations
needed before generation activation; it never creates an issue, project, or
planning source.

Preview a same-document pinned-commit locator migration:

```sh
workstreamctl root-transition plan-url GEN-123 \
  --to https://github.com/owner/plans/blob/main/PLAN.md
```

Preview reopening the exact terminal root to a reviewed Linear `started` state:

```sh
workstreamctl root-transition reopen GEN-123 --state-id <linear-state-uuid>
```

Review `intent`, `authenticated_route`, `expected_snapshot_sha256`, and
`expected_frontier_sha256`, then repeat the identical command with:

```sh
--expected-snapshot-sha256 <preview-value> \
--expected-frontier-sha256 <preview-value> --apply
```

Apply reauthenticates the workspace, team, project, immutable root, and target
state; claims a deterministic append-only reservation; rereads immediately
before `issueUpdate`; and validates exact route, state or description, and
frontier afterward. Repeating the exact apply is a zero-write recovery path.
Zero or multiple canonical URLs, a different document, stale review digests,
route drift, nonterminal reopen, a non-`started` target, or a competing intent
refuses without a mutable write.

Linear does not expose conditional `issueUpdate`. The reservation serializes
clients following this protocol, but cannot make the native write atomic
against unrelated writers. A contradictory post-read therefore fails closed
and requires human reconciliation; the command never silently retries or rolls
back mutable issue data.
