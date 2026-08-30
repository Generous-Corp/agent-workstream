---
name: workstream-ledger
description: Preserve and update evolving project goals, tasks, decisions, dependencies, PR state, evidence, and closure in a durable Linear-backed workstream. Use directly for new plan intake, or after workstream-resume has returned a bounded authoritative snapshot in this turn. For any handle-led turn, including mutation, audit, or closure, do not load this skill first.
---

# Workstream ledger

Keep business logic in a durable issue graph so continuation never depends on
remembering the old chat. Linear is the pilot execution authority; a private
planning document remains the detailed design authority when one exists.

Resolve the directory containing this `SKILL.md` as `WORKSTREAM_SKILL_ROOT`
before invoking a bundled helper. Never assume a user-level skills directory or
a particular plugin cache path.

### Scope and routing identity

Every root carries an explicit namespace plus its immutable Linear root issue
ID and explicit workspace, team, and project IDs. Titles and local checkout
paths are never routing identity. Record
repositories by the source provider's immutable repository ID when available.
Keep the canonical remote coordinate (`host/owner/repository`) as routing and
display data, without assuming one host or owner; local machine/worktree paths
belong only in execution and resume metadata. Preserve renamed/transferred
coordinates as aliases plus verified identity-update events. A redirect never
creates a second repository identity. Coordinate-only fallback requires
explicit redirect-resolution evidence and fails closed when equivalence is
unproven.

For a GitHub rename or transfer already represented by one projected scope,
use the no-model `workstreamctl repository-identity --request REQUEST.json
--apply` path. It authenticates both the exact old coordinate and GitHub's
returned canonical coordinate, requires the same immutable repository ID, and
accepts only one redirect hop. The request fences the material revision,
projection revision, active scope event ID, and scope value digest. It appends
one scope replacement containing the immutable identity-update event; exact
replay is a zero-write no-op. A stale frontier, recycled alias, multi-hop
redirect, or coordinate owned by another repository refuses before mutation.
The identity writer and material/checkpoint writers serialize through one
durable Linear boundary reservation. Material work cannot pass a pending
identity intent; a competing material write that wins the boundary first
causes the identity update to refuse with zero identity writes.
Reservations carry the full immutable projection intent and both authenticated
frontiers. Only an exact deterministic remote slot can block; malformed,
oversized, arbitrary-ID, or stale-plan markers are quarantined into the next
frontier. The exact projection event or a durable authenticated successor
releases the reservation without a time-based lease.
Chained coordinate history such as A to B to C is intentionally not supported;
it requires reviewed manual consolidation before this single-hop writer runs.

Older projections that contain a legitimate first identity backfill are not
silently trusted. Resume returns bounded `partial_reconcile_required` metadata
with no scope, worktree, next action, or execution authority. Use
`workstreamctl repository-identity-seal --request REQUEST.json` for a zero-write
preview, review its exact frontier, then add `--apply`. The writer reauthenticates
the canonical planning source plus the current GitHub identity of every stored
coordinate, serializes at the shared material boundary, and appends one
deterministic v2 seal over the exact legacy projection receipts. Repeating the
same repair is a zero-write readback; altered history, ambiguous candidates,
provider mismatch, stale frontiers, or a second seal refuse.
The exact bounded request and preview/apply procedure are documented in
[`references/identity-history-seal.md`](references/identity-history-seal.md).

Neither a provider ID nor a Linear destination is trusted merely because it is
nonempty. Repository identity includes a timestamped authenticated-provider
readback binding the immutable ID to the resolved current coordinate; every
alias update binds the exact old requested coordinate to that same ID/key and
the resolved current coordinate. Linear routing
includes a timestamped authenticated readback binding workspace, team, project,
and immutable root issue ID. Mismatches and missing verification fail closed.

Use one root across several repositories only when they deliver one goal. Name
one primary repository, every participating repository, repository-qualified
exact heads/evidence, and the repository owner of every child. Otherwise use
separate roots connected by typed `blocks`, `blocked_by`, or `related` links to
immutable Linear workspace + issue IDs (with the `<TEAM>-*` token retained only as
a route/display value). `scripts/workstream_scope.py` validates this logical
contract and rejects missing destinations, unowned children, self-links,
duplicates, unknown relation types, dangling immutable targets, contradictory
directed edges, and missing `blocks`/`blocked_by` inverse views. `related`
remains informational. Closure consumes authenticated peer-edge readback and
refuses while the current root has an active `blocked_by` relation.

Dependencies between already-owned children use the separately bounded
`scripts/workstream_child_dependencies.py` transport. It authenticates the
declared route, immutable root, and both direct-child identities, fences the
root material/projection frontier plus the child dependency graph, reserves a
deterministic append-only `child_dependency_authorization` projection slot,
then projects that immutable authority into a native `blocks` cache relation
with one deterministic client-supplied UUIDv4. The authorization is ordered
after the exact pre-grant material/projection/graph state; the graph frontier
includes a canonical SHA-256 so a same-count edge replacement cannot pass as
unchanged. Root comments observed beyond the reviewed material frontier must
have a strictly later server creation time than the grant. Projection CAS
orders later projection events after it. Events ordered after the grant do not
retroactively invalidate it: contradictions or scope changes require an
append-only superseding event and reconciliation of the derived native cache.
Runtime schema introspection must confirm that `IssueRelationCreateInput.id`,
the `blocks` enum, archived-inclusive relation connections, and global relation
slot enumeration remain available. Complete pagination must recover the exact
active `blocks` view on the blocker, inverse `blocked_by` view on the blocked
child, and any occupied or archived deterministic slot before mutation. A lost
response converges and an exact batch replay performs no comment or relation
write. Ambiguous, duplicate, conflicting, self, cross-root, stale-frontier,
archived-slot, partial-inverse, and mismatched readbacks fail closed. A truly
never-seen UUID cannot be reserved or dry-run through Linear, so availability
cannot be proven beyond complete occupied-slot preflight; deterministic create
and authoritative readback bound that API limitation safely. The transport
never creates or updates an issue, project, or workstream root.

The supported invocation is
`python3 scripts/workstream_child_dependencies.py --request REQUEST.json --apply`.
The JSON object must contain exactly `schema_version`, `authority` (including
the explicit workspace/team/project/root UUID and root identifier),
`plan_revision`, the complete `owned_children` identity set, `relations`, and
the reviewed `expected_frontier`. The command does not infer a route or child
set and emits the complete JSON receipt on stdout.

The live Linear graph transport verifies the declared workspace/team/project
relationship, fences reads to that project, and assigns the project on creates.
The append-only projection transport persists scope and typed cross-workstream
relations as immutable Linear comments and derives their current view from a
complete paginated readback. Replacing a keyed projection must name the exact
event it supersedes; ambiguous concurrent replacements fail closed. The
source digest must equal the root plan revision, and the projected
workspace/team/project/root-issue route must equal the authenticated token
readback. Projection events are reduced within the current root plan revision:
older generations remain immutable history and are counted as stale, but
cannot supersede or conflict with the current generation. Conflicting writes
within the current generation still fail closed. Full-authority resume also
fetches the exact plan bytes and requires their digest and immutable identity
to match the projected source. Evidence contracts are keyed by stable slice ID, so
one child may own several independently verifiable slices. The
deterministic validator rejects missing or mismatched receipts; the transport
remains responsible for their authenticity.

### Markdown plan intake

When a user says `execute this` and supplies a Markdown path, URL, or pasted
plan, snapshot the exact bytes before interpreting mutable status. The
model-free helper emits a root identity, exact revision, and deterministic
child candidates for review:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_plan.py" ./PLAN.md
```

For a private plan already present in an authenticated checkout, provide its
canonical durable URL so the same plan resolves to the same root on every
machine:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_plan.py" \
  ./PLAN.md --identity https://github.com/org/private-plans/blob/<40-hex-commit>/PLAN.md
```

Pasted Markdown works too. First commit the exact text to its durable planning
repository, then run the committed checkout path with its canonical URL as the
identity. If persistence must follow intake, exact bytes may be piped on stdin
with `-`; supply `--identity` as soon as a durable reference exists. An omitted
identity makes stdin content-addressed and is only a provisional snapshot.

The output's `source.sha256`/`root.plan_revision` is the plan revision to record
on the single Linear root. `workstreamctl plan` is intentionally side-effect free and
sets `graph_review_required=true`: Markdown structure can produce deterministic
candidates, but cannot prove which sections are independently actionable. The
active agent must review completeness before creating the root and accepted
children. Apply that explicit review with:

```sh
workstreamctl intake ./PLAN.md --identity <canonical-url> \
  --plan-revision <source-sha256-from-preview> \
  --root-stable-key <root-stable-key-from-preview> \
  --accept-key <stable-key-from-preview>
```

Use `--accept-none` only after explicitly reviewing a root-only graph. When the
repository root contains `.workstream.json`, load its
validated Linear workspace/team/project route automatically; an explicit
`--config` or `WORKSTREAM_CONFIG` may select a deliberate external declaration.
Explicit route arguments must match the declaration, and authenticated route
validation must pass before any issue read or write. Initial root and child IDs
are deterministic UUIDv4-shaped values scoped by immutable
workspace/team/project plus stable plan keys, matching Linear's live validator.
Concurrent duplicate creates reload and validate every owned field,
converging only on an exact match; they never lock by time, update, or delete.
An exact repeat is a zero-write no-op, and the same plan revision may add a
newly reviewed missing child. Changed-plan and other existing-graph mutations
still fail `remote_cas_unavailable` until a remote CAS authority exists. Never
infer a repository, worktree, or Linear project from the Markdown's cwd.

For an existing legacy root that has no deterministic intake marker, never run
initial intake again. After authenticated resume and graph review, add exactly
one missing child through the separately fenced extension command:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_extend_child.py" ./PLAN.md \
  --identity <canonical-immutable-plan-url> \
  --plan-revision <authenticated-source-sha256> \
  --workstream-id GEN-123 --root-issue-id <root-uuid> \
  --candidate-key <reviewed-stable-key> \
  --material-revision <live-material-revision> \
  --projection-revision <live-projection-revision> \
  --state-id <linear-state-uuid> \
  --assignee-id <linear-user-uuid> \
  --workspace-id <uuid> --team-id <uuid> --project-id <uuid> --apply
```

The command requires exact plan bytes, route, root identity, reviewed candidate,
both live frontiers, a canonical native state UUID, and exactly one of
`--assignee-id` or `--unassigned`. Before the grant, authenticated Linear
readback proves the state belongs to the exact team and an active assignee
belongs to the workspace/team; the grant binds that validation digest. The
append-only grant binds the current generation selection proof and native setup
before one atomic deterministic child create. A sealed
retirement prefix preserves an exact granted create/replay while refusing new
retired-generation grants. Readback must match state and assignee. A replay
converges; it never updates an issue, root, or project. Project the child's
repository ownership before treating it as executable.
If a validated provider identity disappears after the grant, automatic
correction is unsafe without a provider conditional or positive writer-death
fence. Restore the exact identity and replay; otherwise stop for explicit
review. Never supersede merely because the deterministic child is absent.

### Append-only plan-generation authority

An existing root changes plan generations only through
`scripts/workstream_generation.py`; never update its issue description to make
a new digest authoritative. Before the first generation-control event, a
legacy root with exactly one description `Plan revision` remains compatible and
that line selects its plan. Once a schema-v2 genesis or activation exists, the
append-only control chain is authority and description prose is diagnostic
only.

For a legacy root that has no description plan revision, authenticate the exact
route and plan source and bootstrap one deterministic genesis:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_generation.py" \
  bootstrap GEN-123 --plan-source ./PLAN.md \
  --plan-identity <canonical-immutable-plan-url> \
  --created-at <reviewed-utc-time> --apply
```

Do not bootstrap a description-backed root. For a reviewed plan replacement,
first build the complete candidate projection, then provide a structured
retirement proof for the current active epoch:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_generation.py" \
  activate GEN-123 --plan-source ./PLAN.md \
  --plan-identity <canonical-immutable-plan-url> \
  --retirement-proof ./retirement.json \
  --created-at <reviewed-utc-time> --apply
```

When the reviewed replacement needs a root checkpoint to keep the ordinary
resume surface within its default budget, provide that pending checkpoint and
the authenticated repository head used for disposition:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_generation.py" \
  activate GEN-123 --plan-source ./PLAN.md \
  --plan-identity <canonical-immutable-plan-url> \
  --retirement-proof ./retirement.json \
  --activation-checkpoint ./pending-checkpoint.json \
  --remote-head <authenticated-exact-head> \
  --created-at <reviewed-utc-time> --apply
```

The checkpoint must cover the exact current root material revision, name the
target plan revision, remain pending, and pass the standard 24 KiB resume
budget; custom resume budgets are refused for this form. The target disposition
may be prepared first, but the checkpoint is inert until the authenticated
generation transition is appended last. That same transition becomes its
remote acknowledgement. A crash before the transition leaves the predecessor
authoritative; exact replay converges without a second checkpoint or control
event. Only checkpoints carried by the uniquely selected authenticated
generation chain are reduced. A forged, forked, off-chain, conflicting, or
duplicate physical copy fails closed.
Keep the pending checkpoint file until the command returns its successful
authority-bound post-read receipt. If execution stops after preparing the
target disposition, resume with that exact file; a plain or regenerated
checkpoint retry refuses rather than activating an unrecoverable disposition.

The command uses the normal strict full-resume validator against authenticated
source bytes and route readback, with the resume defaults of 24 KiB and 100
items unless matching `--max-bytes` / `--max-items` values are supplied. The
strict check validates complete history but emits the normal compact-validated
resume surface; it does not require a full-history output artifact. It
binds the actual material, checkpoint, issue-graph, and projection frontiers.
Mutation order is fixed: reserve the
shared material/checkpoint boundary, append the target candidate seal, reread
and strictly validate, then append the predecessor activation last. Competing
candidates use the same route/root boundary slot. Material and checkpoint
writers cannot pass a pending reservation. Old runtimes which try to route
around that occupied slot through deterministic collision successors are
quarantined from both reduced frontiers; upgraded active writers use the
completed-generation frontier token. A sealed candidate cannot change before
activation, and a retired epoch's production writers refuse before mutation.
Quarantined legacy ledger writes remain non-authoritative, but resume and
generation receipts surface their stable count and digest for diagnosis;
upgraded active-generation writes are not counted. After activation, the active
target may evolve normally; its sealed prefix remains the activation proof. The
CLI performs another authenticated strict
full-resume read of the actual active generation after authority changes before
reporting success. A same-generation bound/live graph or candidate digest
mismatch refuses with `authority_changed_with_post_read_drift`; authority has
already changed and the command does not claim rollback or cross-resource
atomicity. An exact historical replay returns the original receipt without
writing even after later successors, then validates and reports the current
active successor rather than forcing the historical target.

The retirement proof is not a boolean. It names the predecessor plan, writer
epoch, complete provenance and checkpoint event-ID sets, retirement time, and
its canonical declaration digest. A partial operation remains durably
recoverable from its exact reservation. If review decides it must not continue,
use the `activate` command's exact `--abort-reservation-id`,
`--abort-reservation-sha256`, and `--abort-reason` form; abort releases only
that reservation and never changes generation authority. Abort is a validated,
non-authoritative projection event, so it advances the predecessor CAS frontier
without advancing the generation authority chain. Abort and final
activation compete for the same deterministic predecessor authority slot, so
an abort that wins the final race cannot be followed by that activation. If an
unrelated predecessor writer wins the original slot first, abort binds the
complete intervening event-ID frontier, digest, and original occupant, then
rebases to the current next predecessor slot with bounded reload/retry. A
reviewed replacement can reserve and activate from the revision advanced by
that abort. The generation
transport uses `commentCreate` only and never calls `issueUpdate`.

After intake returns the canonical root token, invoke
`scripts/workstream_tab.py GEN-123`. In cmux or Herdr it preserves the existing
tab title and appends exactly one token; the same token is a no-op and a
different token refuses without mutation. Herdr resolution is allowed only
inside `HERDR_ENV=1` with the inherited exact tab, workspace, and socket
namespace; never query a focused/default session or treat a bare `w1:t1` as
globally unique. Outside a supported manager, or when the exact target cannot
be resolved, it reports an optional no-op without changing the preceding
full-authority recovery result.

### Material-delta journal

User turns are not the only source of durable work. When an agent discovers a
blocker, requirement, decision, or follow-up, append a structured delta to the
local journal before attempting the remote Linear mutation. The model-free
journal is available as `scripts/workstream_delta.py` and exposes a
`DeltaJournal` plus `MutationAdapter` boundary. Each delta has a deterministic
`event_id`, source, and expected root revision. Reusing an event ID for
different immutable material is an error. `append_boundary` batches all
material changes from one substantive boundary into one row and performs no
write when the change list is empty.

The journal marks a row applied only after the adapter returns a receipt. A
crash after the remote mutation but before that local acknowledgement leaves
the row pending; replay is therefore expected and safe. Never discard a
pending row or advance its revision by last-writer-wins. Both `apply` and
`apply_with_rebase` refuse adapters that declare neither true atomic CAS nor a
lossless append-only event log. `apply_with_rebase` reloads the live revision
and retries a stable event ID after `RevisionConflict`.

`scripts/workstream_linear_events.py` is the dependency-free authenticated
Linear `MutationAdapter`. It stores one immutable material delta per issue
comment using Linear's documented `commentCreate` API, paginates the complete
comment set, and derives the ledger revision from the valid event set. It does
not use or claim conditional `issueUpdate`. Material events and checkpoints
share one deterministic client-supplied boundary slot derived from the stable
workspace/root identity, current material revision, and checkpoint frontier.
Team or project moves therefore cannot create a second slot. A checkpoint
winner moves a concurrent event to the next frontier; an event winner forces a
pending checkpoint to reload and rebuild. Replay returns the existing receipt;
bounded rebase replay also accepts the same stable event ID when workstream,
kind, source, payload, and creation time are identical and the remote expected
revision moved only forward. Reverse revisions or any other material mismatch
remain conflicts. This closes the crash window where remote acceptance occurs
after rebase but before the local outbox acknowledgement. Duplicate or
conflicting event IDs,
malformed markers, causal revision gaps, incomplete pagination, missing auth,
and unobserved writes fail closed. Configure it with `LINEAR_API_KEY` and the
exact root issue identifier. `from_env` also consumes the validated
repository-root `.workstream.json` route when present and refuses an issue
outside that workspace/team/project. Do not use a locally cached comment subset
as the reducer input.

This is logical transport proof, not physical remote proof: deterministic fake
tests cover concurrency and no-loss behavior, but they do not prove a live
Linear workspace, a second machine, process death, API consistency, permission
scope, or resistance to an authorized person editing/deleting comments. No
live Linear mutation is part of the test suite. A local journal still proves
process-restart replay on that machine only, not recovery after the machine
disappears.

Version 0.4.31 closes an incident in which two callers encoded flat progress
payloads while labeling them `material_boundary`. New journal and remote writes
now require a nonempty boundary ID and one or more exact `{kind, payload}`
changes. Historical envelopes remain inspectable, but resume will not interpret
a malformed boundary unless a later reviewed `material_semantic_repair` control
binds every malformed event and the exact material, checkpoint, projection,
generation, route, and plan-source frontiers. The repair is append-only: it
overlays normalized boundaries at their original positions and never edits or
deletes the original comments. Preview the reviewed manifest first, then apply:

```sh
workstreamctl material-repair GEN-123 --manifest repair.json \
  --review-artifact reviewed-targets.json \
  --plan-source ./PLAN.md --plan-identity <canonical-url>
workstreamctl material-repair GEN-123 --manifest repair.json \
  --review-artifact reviewed-targets.json \
  --plan-source ./PLAN.md --plan-identity <canonical-url> --apply
```

The reviewed target file is authenticated locally by immutable commit/path and
SHA-256 and fetched through the authenticated source-byte loader; fetched and
local bytes must match before Linear access. Normalization is mechanically lossless: exactly two incident targets,
`repair:<event-id>`, and one `progress` change containing the complete original
payload. The material comment uses one deterministic slot; the complete
cross-surface readback is a preflight/postcheck fence, not transactional CAS.
The repair-only adapter performs another combined read immediately before the
create, requires the exact pinned serialization frontier, and writes only the
reviewed slot—never a recomputed successor slot.
Later authorized generation, source, or child evolution does not invalidate the
immutable historical repair proof.
The control kind is reserved from generic journal/encoder/adapter paths.

### Durable choice events

Material choices made where the specification was silent are typed immutable
events, not mutable prose. `scripts/workstream_choices.py` constructs and
reduces `recorded`, `audited`, and `superseded` events. Every event carries the
stable choice and workstream IDs, owning child, namespace, canonical repository
coordinate, exact plan revision, exact Git head, and timestamp. A recorded
choice separates technical confidence from confidence that the owner would
make the same choice; it also records alternatives, reach, reversibility,
affected domains, and evidence.

Never rewrite or delete an earlier choice event. Append an audit or
supersession and derive the current view. Security, authority, persistence,
concurrency, release, fleet, and irreversible choices require a fresh-context
read-only audit and cannot remain provisional. Every active material choice
requires an explicit audit verdict before closure. Any active `must_fix` choice
blocks landing; reversible low-risk choices may receive a provisional verdict
with a review trigger.

Choice events use the same fully paginated append-only projection boundary as
scope, relations, evidence contracts, source, provenance, and continuation
disposition. Their immutable event identity remains intact inside the projection
event. This is authenticated transport proof, not evidence that an authorized
person cannot edit or delete a Linear comment.

### Slice evidence contract

For every substantial independently verifiable child, maintain the layered
contract checked by `scripts/workstream_evidence.py`. Name the owned seam, trust
boundary, allowed side effects, plan revision, immutable repository key, and
exact full Git object ID. Every required receipt repeats that repository key and
head plus its proof kind and successful outcome. For every layer,
either attach receipts or explain why it is not applicable:

- unit/property/model/oracle proof for tricky logic;
- component/seam proof, with fakes only at external edges;
- adapter contract-fake proof distinguished from a bounded live canary;
- bounded true end-to-end journeys where they add coverage;
- screenshots as supporting visual evidence, never primary behavior proof;
- operational receipts bound to the exact head; and
- a negative control proving the evidence instrument detects failure.

Do not call a system test with mocked providers live end-to-end proof. A head
change invalidates head-bound receipts until reconciled.

The labeled canonical URL must be the exact authenticated source identity,
including its Git ref, and its digest must match the structured projection.
Changing a URL or revision is a synchronized source transition, never an
implicit identity alias.
When legacy completed ownership already blocks full authority, a reviewed
`terminal_child_source_transition` may perform only the same-document
`blob/main` to immutable 40-character commit transition at the same digest.
It freezes every non-source head and disposition, fences the exact pending
children, and returns partial authority until evidence seeding and closure.

### Terminal-child projection repair

After a material child transition to Linear's `completed` state, run the
model-free projection reconciliation before treating resume as authoritative.
A reviewed `terminal_child_repairs` batch must fence every exact child issue
from one authenticated root snapshot, including each
parent/workspace/team/project route, exact assignee state (including explicitly
unassigned), state ID/name/type, and canonical
readback digest. The repository owner and head are derived only from named,
active, valid evidence-contract heads and must already exist unchanged in the
current scope. Closures are appended in canonical child order before any
required scope replacement. A crash may resume only from that exact prefix.
The writer never creates, reopens, or updates an issue.
New closures use schema v2 so explicit unassignment is representable; readers
continue to accept schema-v1 closures only with their original nonempty
assignee identity.

For a legacy completed, already-owned child whose receipts are still present
but were never projected as evidence contracts, use a separately reviewed
`terminal_child_evidence_seeds` phase first. It may only append the named,
valid contracts whose repository and exact head already match scope. It remains
non-authoritative until the subsequent closure-repair batch succeeds; the two
phases cannot be combined, and either phase is revision-checked and idempotent.
When a new plan generation begins at an exactly empty projection frontier, a
reviewed `terminal_child_evidence_seed_predecessor` may instead carry only
contracts that an exact predecessor-generation closure authorizes. The binding
fences the complete stale projection history, predecessor projection and tip,
material log, predecessor checkpoint chain, and graph/material/checkpoint input
frontier. A mixed predecessor is accepted only through the normal authenticated
reducer when one v2 `cas_activation` binds the exact ordered legacy-v1 IDs and
digest, every later event is v2 on the authenticated route, and no event is
quarantined; the binding digest and frontier cover the complete accepted mixed
history. The carried contract may change only its plan revision; its historical
head, receipts, owner, and repository remain immutable. The carry proof is
persisted on the new evidence event, so the later closure repair and ordinary
resume can revalidate it against the predecessor history. Missing, ambiguous,
mutated, unclosed, late, or frontier-drifted evidence refuses before mutation.
When the reviewed seed also advances the primary repository head, every seed
must belong to that primary repository and bind the new head. The manifest
records the exact predecessor/new heads, computed disposition, checkpoint
identity, and issue/material/checkpoint frontier. The writer fences that
frontier before every append and commits in evidence, disposition, scope order,
with scope last. Existing closed-child evidence may retain an older head only
when immutable projection order proves the complete evidence set and scope were
valid when its closure was appended; open or unclosed evidence always remains
bound to the current head.

An exact replay is a no-op. An incomplete multi-child batch, missing/failed
receipts, multiple owners or heads,
non-completed state, missing assignee, route/readback drift, unrelated scope or
source changes, and stale evidence refuse without mutation. Resume validates
the closure against current Linear state, current repository identity/ownership,
its creation-time scope and evidence set, and receipts
before returning full authority. Once a completed child is present in the
structured ownership map, full resume requires its active closure even if its
evidence is later retired. Every closure creation or replacement requires the
matching reviewed repair and live readback fence.

### Truth and closure reducer

`scripts/workstream_state.py` supplies the deterministic adapter boundary for
the remaining P0 checks. `apply_delta` is a revision-fenced compare-and-swap;
`reconcile_external` records live-head contradictions and projects a merge to
`Landed — acceptance review required`, never directly to semantic `Done`;
`closure_errors` names missing children, plan drift, stale receipts, unowned
blockers, and open work hidden under a completed parent. A Linear adapter must
persist the returned state with the same revision fence and must not normalize
these errors away. Deterministic closure evidence without an explicitly passed
semantic review remains `Landed — acceptance review required`; invocation alone
cannot emit `Done`.

`workstreamctl reconcile` is the bounded live landing/closure path. It reads
every participating authenticated GitHub repository identity,
repository-qualified PR head, and merge SHA; invokes one
explicit fixed-argv Shipyard receipt adapter without a shell; and persists the
aggregate result as the singleton lifecycle projection. Repeatable
`--repository-binding` JSON groups are the multi-repository interface; the
original single-repository flags remain compatibility sugar for one group.
Drift invalidates evidence only for that repository but blocks aggregate
closure. Projection revisions use
route-scoped deterministic client-supplied Linear comment IDs as exclusive
remote slots. Runtime schema introspection must confirm that Linear still
supports `CommentCreateInput.id`; an identical collision is replay, while a
different winner requires reload and review before another write. Reviewed v1
history is fenced by one v2 activation event; later v1 writes are quarantined
and surfaced by count/digest (or exact events in history mode) rather than
silently admitted. Lifecycle reconciliation refuses while any quarantine is
unresolved; retiring one requires a reviewed projection disposition naming the
exact event IDs and event digest. A merged exact head set can emit only `Landed —
acceptance review required`. `Done` additionally requires a durably projected
fresh-session review receipt bound to the exact snapshot, closure input,
repository-qualified head set, and aggregate landing truth. The receipt names and digests the durable review
artifact and declares procedural independence under a shared Linear credential;
it does not claim cryptographic agent identity.
Resume derives lifecycle status and the closure receipt digest from that durable
projection rather than mutable issue prose.

### Fresh-session resume

If `workstream-resume` already returned the bounded authoritative snapshot in
this turn, retain it and do not repeat initial recovery.

When a new agent receives `ABC-123` (or its Linear URL/tab title), with or
without continuation instructions, the first action is the default bounded
resume helper:

```sh
python3 "<absolute directory of the SKILL.md loaded for this turn>/scripts/workstream_resume.py" ABC-123
```

Substitute the runtime-supplied loaded skill path directly. Do not search the
filesystem, inspect cwd/environment, probe `PATH`, or execute the placeholder
or an unset variable. Run it before reading repository instructions, memory,
local worktree lists, or PR state: the result identifies the repositories and
nonterminal work that are actually in scope. Do not probe `workstreamctl` on `PATH`; it is a
repository-local convenience command and plugin installation does not add a
global executable. The initial recovery command always omits
`--include-history`, even when the request ultimately includes audit or closure.
The default validates the complete history and returns the actionable current
view. Run a second full-history invocation only when actually beginning that
later audit or closure pass.

The helper obtains one root snapshot plus its nonterminal children from Linear.
The resolver extracts exactly one distinct
token from a bare token, Linear URL, natural-language request, or copied tab
title. It validates the context URL, exact plan revision, root revision, child
uniqueness, and root/child next actions, then enforces both item and byte caps
while excluding terminal children. A tab title is only a token carrier; no
agent may resume from a cwd, stale transcript, or title metadata alone.

The current paginated Linear transport obtains the issue graph and reads the
root comment connection once to reduce append-only material events, remote
checkpoints, and the complete projection history. The same bounded graph read
collects the first comment page for every nonterminal child and paginates only
continuations; each child is route/identity validated and independently reduces
its own material events and checkpoints. Child state never inherits root events
or another child's events, and malformed, conflicting, or incomplete child logs
refuse the whole resume. Root-authorized children hidden from native
`root.children` by reparent/project drift are recovered by immutable ID before
both ordinary resume and strict generation-candidate validation. A newer material-event
`next_action` supersedes stale root description prose. The projection restores
scope, relations, choices, evidence contracts, source, provenance, and the
recorded attach/successor disposition. An acknowledged checkpoint restores its
bounded machine, worktree, exact head, evidence, blocker, and next action and is
the authority used to choose attach versus successor after live remote-head
verification. The recorded disposition is always the explicit `attach` or
`create_successor` result and must reconcile with that checkpoint and live head;
an ambiguous placeholder is not executable. An empty surface is reported as
empty rather than fabricated.
When no local config is available, authenticated token-only bootstrap resolves
the root's exact workspace/team/project route before the fenced read.

After scope owns the exact child, `workstreamctl child-event` and
`workstreamctl child-checkpoint` append child-local state. Both require the root
token/UUID, child token/UUID, route, selected plan generation, and live
revision/predecessor fences. Each first writes a deterministic inert proposal
containing the full child record, then root projection CAS activates only its
exact ID/digest after fresh generation, scope-owner, child-parent, and route
authentication. That native child check is a drift preflight, not the ownership
proof or an atomicity claim. Mutation authority is bound to immutable root-side child origin:
the exact `child_extension_authorization` event/digest and deterministic
root/route/candidate child UUID, or deterministic initial-intake root/child
markers and IDs. Native parent/team/project fields are only a cache; drift never
transfers ownership and is reported as a separate reconciliation blocker.
Resume excludes unactivated proposal business payload from status, blocker, and
next action, but emits bounded pending handles containing child token/UUID,
proposal ID/remote ID, kind, and record digest. Root metadata contains no child
business payload. Crash before activation leaves a recoverable inert proposal;
`workstreamctl child-proposal-activate` reads that exact proposal by its pending
handle and root-CAS activates it without the original payload or checkpoint
file. Crash after activation makes that exact proposal authoritative.
Proposal decoding validates the supported kind and complete canonical event or
checkpoint record before exposing even a pending handle. Every activated grant
revalidates its immutable child origin before its business record is reduced.
Exact replay is zero-write, including after sealed generation retirement or a
later scope removal; an unowned new or mismatched child refuses.

Legacy 0.4.29 `child_extension_authorization` replay validates the exact
deterministic existing child and writes nothing without consulting current
workflow-state or assignee providers. Deleted provider identities therefore do
not invalidate an old completed creation; a missing legacy child still refuses
before any create.

The direct resume helper is full-authority by default and fetches the projected
source identity. `--plan-source` overrides the fetch location for an
authenticated checkout; `--plan-identity` preserves its durable identity. It
refuses success unless those exact bytes match the root and projection.
The default bounded context validates the complete Linear history, preserves
current child identity, status, next action, blockers, worktree/checkpoint
identity, and exact uncheckpointed requirements, decisions, and follow-ups.
Its `context_schema` is `agent-workstream.resume-context` version 2 with
`representation: compact_validated`; verbose descriptions, receipt bodies,
checkpoint evidence, provenance, and terminal readbacks are represented by
counts, digests, exact repository heads, and active projection-head bindings
instead of being duplicated into every agent prompt.
Compact provenance uses the same candidate predicate as full history: every
provenance item with a truthy worktree is a candidate. Exactly one is preserved,
including a dirty or stale predecessor; multiple candidates refuse, and
projection order is not implicit supersession.
After initial bounded recovery, an explicitly requested audit or closure pass
may use a second `--include-history` invocation and raise the explicit byte/item
caps when that complete history is known to exceed the normal resume budget.
That response uses the same schema version with `representation:
full_validated` and preserves the complete validated values; required current
state is never truncated.
For an immutable `github.com/.../blob/<40-hex-commit>/<path>` source, HTTPS is
tried first; a 404 may fall back to existing noninteractive GitHub SSH access
in a temporary isolated repository. A synchronized living plan may use the
canonical `blob/main` identity: the fallback snapshots one `FETCH_HEAD`, and
resume still refuses unless those bytes match the root's recorded SHA-256.
Other mutable refs, malformed paths, prompts, timeouts, and Git failures refuse
resume rather than weakening source proof.
Any positional JSON snapshot requires `--inspection-only`, labels its output
`inspection_only`, and is not authority to continue work; JSON fields that
claim authentication do not change that boundary. Full authority comes only
from the command's live authenticated Linear read. `workstreamctl projection`
requires a reviewed manifest containing the exact current projection revision,
the exact active key/event/value-digest set, and explicit retirements naming
their reviewed event and value digest. It never retires an omitted key. A late
key or changed head requires reload and review before any append. The command
computes the concrete attach/successor disposition against the reviewed
`--remote-head` and rereads the complete comment stream before reporting
success. It does not itself authenticate Git hosting or refs: the caller must
obtain that head through an authenticated repository read before reviewing the
manifest, and must not describe the CLI argument alone as live remote
verification.
Projection synchronization treats a labeled `Canonical plan: <URL>` line on
the existing root issue as the source identity. Exactly one distinct URL is
required before any write; zero or multiple candidates refuse with a concrete
remediation and never create another issue or project. A legacy active plan
still requires the supplied authenticated identity to equal that exact URL.
An inactive, unselected, non-retired generation candidate may instead use an
immutable revision URL for the same host/owner/repository/path only when its
reviewed target projection contract is exact and its explicit source item
matches the authenticated identity and SHA-256. Once activated, normal resume
and projection trust that structured active source rather than description
prose. The original canonical URL and full description digest are diagnostic
fences: both are reread before the first append and after final validation, and
the command never calls `issueUpdate`. A different document, ambiguous label,
source mismatch, stale review frontier, or description drift refuses. The
command otherwise adds a missing structured source or refreshes its identity
and SHA-256 while preserving the issue and append-only history. Repeated
synchronization with a current review contract writes nothing. Intake does not
add a labeled canonical URL by itself; any issue mutation that adds or changes
that line must finish through this projection transaction. A projection
operation is not reported successful until the same strict bounded validation
as resume returns `resume_authority: full`.
If a historical relation points to a peer created before the relation-readback
contract, only this reconcile command may load that incomplete head, and only
when the reviewed manifest exactly retires or replaces every incomplete
relation. Those relation migrations are appended before unrelated changes;
normal resume and final readback remain strict.

This remains a validated bounded snapshot, not complete physical cross-machine
recovery proof. Resume does not itself fetch owner, live source-control truth,
landing-controller truth, or deletion resistance. Reconcile plan revision,
repository scope, and exact heads before implementation. An unfetched surface
still emits `transport_unimplemented`, and closure treats that marker as a
blocker.

### Linear graph operations

`scripts/workstream_graph.py` converts an intake payload plus a reviewed set of
candidate keys into deterministic root/child operations and refuses child
creation before explicit review. Stable keys make sequential comparison and
same-revision no-op detection deterministic. The authenticated intake transport
maps those keys to client-supplied, route-scoped UUIDs and resolves a duplicate
create only by complete reload and exact-field validation. Focused fake-client
tests cover concurrent first creation and collision refusal; they are not live
Linear or cross-machine proof. Existing-graph mutations that require remote CAS
remain refused.

### Material-boundary checkpoint schema

`scripts/workstream_checkpoint.py` defines deterministic checkpoint identity,
required execution/worktree fields, remote-ack validation, plan-drift checks,
and complete predecessor-chain recovery. `scripts/workstream_linear_checkpoints.py`
persists that schema as a distinct immutable marker over the same authenticated,
fully paginated Linear comment boundary as material-delta events. It derives an
acknowledgement only after readback, replays an already observed event without a
second write, and fences the exact live material revision and checkpoint
predecessor through the shared ledger-boundary slot before appending. A
checkpoint is linearized when it wins that slot. An event admitted after the
checkpoint's final combined read is later, uncheckpointed work and resume
surfaces it as such. It fails closed on ambiguous or malformed remote state.
Legacy comments may have arbitrary remote IDs and remain readable; new writes
use deterministic slots. A legacy checkpoint whose root revision is ahead of
the material-event history is not guessed or silently repaired: new event and
checkpoint mutation fails with `checkpoint_material_history_incomplete` until
a reviewed quarantine/remediation operation accounts for the missing history.
Exact replay of the existing checkpoint remains a no-write acknowledgement.
This is
logical transport proof. Its `from_env` constructor consumes the same validated
repository-root route and refuses a root outside it. Do not call the
deterministic fake-client tests a live Linear, second-machine, process-death, or
deletion-resistance canary.

## Start or restore

1. Size the work.
   - Track work that may cross a session/machine, creates a PR, leaves an open
     obligation, or cannot finish atomically in the current turn.
   - Skip a truly small change completed and verified now with nothing left.
   - Promote a growing task without changing its stable workstream ID.
2. Resolve one workstream and durable context URL. For a plan-backed project,
   link the exact private plan section. For small durable work, the Linear issue
   itself may be the goal.
   Record the namespace, explicit Linear workspace/team/project destination,
   canonical repository coordinates, child repository ownership, and typed
   cross-workstream relations before mutation.
3. Load validated `.workstream.json` from the exact repository root when it
   exists. Treat it as declared routing authority, not as a cwd-based guess; do
   not substitute similarly named workspaces, projects, teams, or repositories.
4. On recovery, read the full nonterminal issue graph, dependencies, decisions,
   comments, plan link/revision, and next action. Then query live source-control
   and landing-controller state when those capabilities exist; neither old
   comments nor local state are PR truth.
5. Report stale contradictions before implementation:
   - open issue whose PR is merged/closed or whose head changed;
   - waiting issue with no blocker/owner/review date;
   - completed parent with nonterminal children;
   - cancelled decision still present in acceptance criteria;
   - PR without a workstream, exact head, provenance, or landing owner.
6. Only when a stable external ingress integration and its private transport
   are already configured, recover turns that arrived after the last structured
   checkpoint:

   ```sh
   python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" flush
   python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" \
     recover --workstream ABC-123
   ```

   Classify each material event into one reviewed bounded promotion request,
   then use `workstream_ingress.py promote --request <json> --apply`. The
   command durably stages the intent before its deterministic Linear mutation
   and posts the processed successor only after Linear readback. If the source
   disappears after staging, resume with `promote --repo <private-repo>
   --remote-issue <number> --event <wsi-id> --apply`; no source outbox or request
   file is required. A repeated event ID is one logical event. An unprocessed event is evidence that the
   next agent must triage it, not evidence that the request was accepted.
   A normal plugin installation skips this step and resumes from the last
   durable checkpoint. Do not invoke ingress merely to probe whether it is
   configured: that can create local state or fail for missing configuration.

## Maintain on substantive turns

Perform a tiny delta update without a second model:

| Conversation change | Durable action |
| --- | --- |
| New material requirement | Create/update an issue or child with why and acceptance |
| Changed requirement | Update the active item and preserve the old decision in history |
| Spec-silent material choice | Append a typed recorded event to its owning child; never overwrite it |
| Independent choice verdict | Append an audited event; block landing on active must-fix/high-risk unaudited choices |
| Rejected work | Cancel/supersede with date and reason; never erase it |
| New dependency/blocker | Add the relation, exact blocker, owner, and next action |
| PR created/head changed | Attach PR and exact head; invalidate prior-head evidence |
| A landing controller accepts the PR | Attach its exact-head controller receipt |
| PR merged | Close only acceptance that actually passed; retain follow-ups |
| No material delta | Do not write to Linear |

After creating the first durable issue for a session that began unbound, bind
already-captured turns only when an explicitly configured ingress integration
provides an exact event, session, or trusted surface identity:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" \
  bind --workstream ABC-123 --context-url https://linear.app/example/issue/ABC-123/example \
  --surface "$WORKSTREAM_SURFACE_ID"
```

Without trusted surface metadata, use the exact provider session ID from
`recover`; never bind by repository cwd. Multiple sessions
may share one checkout. A bind is persisted, so the session's LATER turns bind
themselves — you do not need to re-bind as the session continues. If a binding
was wrong, correct it without processing the event (this also forgets the
persisted identity, so the next turn does not re-apply it):

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" \
  unbind --workstream ABC-123 --session <exact-provider-session-id>
```

After triage, use `promote` for a material event. Never mark a material event
processed before the promotion path verifies the corresponding immutable
Linear event and exact receipt. GitHub comments for `superseded` or
`no-material-delta` are mutable hints only and never suppress an open capture;
`process` therefore refuses to publish them as durable classifications.

Issue titles must be understandable outside the project view and use a
plan/workstream-derived prefix. Every issue independently includes the stable
workstream ID, plan section when present, namespace, canonical repository
coordinate, why, completion gate, PR, repository-qualified head, optional
landing-controller receipt, evidence, current blockers, and next action.

## Checkpoint before continuation

Before continuing in another agent session:

1. Promote every material delta through the current user turn. Reduce and
   reconcile typed choices; do not close with an active must-fix, unaudited
   high-risk choice, plan drift, or stale exact-head audit.
2. Update plan architecture/scope when needed; do not copy the whole plan into
   Linear.
3. Reconcile repo/branch/head/PR and mark uncommitted or unpushed work as
   machine-local.
4. Build the material-boundary checkpoint and persist it through the remote
   adapter. Do not proceed on a local-only or unacknowledged checkpoint.
5. Record one next action and the durable context's current `updatedAt`.
6. Pass the workstream handle, context URL/update time, plan URL, exact head,
   and PR URL to a continuation adapter only when that capability exists.
7. Require the new session to reread the durable graph and live PR state and
   acknowledge the restored objective, decisions, open items, head, and next
   action. Do not close the source beforehand.

When a configured Shipyard handoff needs a native launch profile, create it
only after step 4 with the bundled product helper resolved from this skill:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_shipyard_profile.py" ABC-123 \
  --repo-path /absolute/worktree \
  --model <canonical-model> --reasoning-effort <effort> \
  --output /owner-only/private-directory/ABC-123.json
```

The command itself reruns authenticated full-authority resume, derives the
provider/session from the current remote checkpoint, and refuses any stale,
uncheckpointed, dirty, mismatched, or unlineaged authority. It emits no prompt
or secret. Owner-only profile publication currently requires macOS. See [the
exact bridge and digest contract](references/shipyard-launch-profile.md).

## Abrupt termination

Native transcript resume is useful but not cross-machine task authority. The
plugin installs no capture hook, so turns after the last remote acknowledgement
are not recoverable unless the user has separately configured a stable external
capture integration. With that optional integration, a local unacknowledged row
still cannot be recovered after its source machine disappears. State that
physical limit honestly.

The ingress is an at-least-once transport, not a second task tracker. Remote
consumers deduplicate capture by `event_id` and promotion by its deterministic
promotion/material-event IDs; Linear holds the promoted business logic.
Local remote-acknowledged rows rotate after 30 days, remote issues rotate by
machine/month, prompts are capped at 16 KiB, and documented credential patterns
are sanitized. See [durable ingress](references/durable-ingress.md) for the
exact privacy and mutation boundary.

Never store credentials, account identities, private filesystem paths, or raw
transcripts in public PR metadata.

## Optional adapters

Session managers may provide stable surface identity and continuation UX;
landing controllers may provide exact-head ownership and receipts. cmux and
Shipyard are examples only. Check that the capability is installed and
configured before using it, and never treat an adapter as the workstream
authority.

Future backup work may add a read-only private flat-file snapshot, but it must
not become an import/sync path or second authority.
