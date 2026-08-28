# Linear comment-slot proof

On 2026-08-28, authenticated Linear schema introspection reported
`CommentCreateInput.id: String`. A disposable comment using client UUID
`6267f4f7-9537-4eae-ad33-7a6580d1f8ce` was created on `GEN-37` and returned
that exact ID. Reusing the ID on `GEN-37` and `GEN-40` both returned transport
collisions, proving IDs are global rather than issue-scoped. The disposable
comment was then deleted successfully; no canary comment remains.

The runtime adapter repeats the schema capability check and derives slots from
the immutable workspace, team, project, and root-issue route.
