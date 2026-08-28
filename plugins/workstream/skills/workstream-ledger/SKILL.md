---
name: workstream-ledger
description: Preserve evolving project goals, tasks, decisions, dependencies, PR state, and next actions in a durable Linear-backed workstream across agent sessions and machines. Use when executing a plan, starting substantive or PR-bound work, changing requirements, recording superseded decisions, detecting stale work, resuming from a stable issue handle, or deciding whether work can close.
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
duplicates, and unknown relation types.

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
not use or claim conditional `issueUpdate`. Concurrent writers observed at the
same revision append independent comments, so neither replaces the other.
Replay returns the existing receipt; duplicate or conflicting event IDs,
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

### Fresh-session resume

When a new agent receives only `ABC-123` (or its Linear URL), obtain one root
snapshot plus its nonterminal children from Linear and pass it through
`scripts/workstream_resume.py`. The resolver extracts exactly one distinct
token from a bare token, Linear URL, natural-language request, or copied tab
title. It validates the context URL, exact plan revision, root revision, child
uniqueness, and root/child next actions, then enforces both item and byte caps
while excluding terminal children. A tab title is only a token carrier; no
agent may resume from a cwd, stale transcript, or title metadata alone.

The current paginated Linear transport obtains the issue graph and reads the
root comment connection once to reduce append-only material events, remote
checkpoints, and the complete projection history. A newer material-event
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

`workstreamctl resume` is full-authority by default and fetches the projected
source identity. `--plan-source` overrides the fetch location for an
authenticated checkout; `--plan-identity` preserves its durable identity. It
refuses success unless those exact bytes match the root and projection.
The default bounded context validates the complete Linear history, preserves
current child details and exact uncheckpointed requirements, blockers,
decisions, and follow-ups, keeps actionable checkpoint evidence and routing,
then returns digests/counts for acknowledged history and validated routing
evidence instead of duplicating them into every agent prompt.
Use `--include-history` for an audit or closure pass and raise the explicit
byte/item caps when that complete history is known to exceed the normal resume
budget; required current state is never truncated.
For an immutable `github.com/.../blob/<40-hex-commit>/<path>` source, HTTPS is
tried first; a 404 may fall back to existing noninteractive GitHub SSH access
in a temporary isolated repository. Mutable refs, malformed paths, prompts,
timeouts, and Git failures refuse resume rather than weakening source proof.
Any positional JSON snapshot requires `--inspection-only`, labels its output
`inspection_only`, and is not authority to continue work; JSON fields that
claim authentication do not change that boundary. Full authority comes only
from the command's live authenticated Linear read. `workstreamctl projection`
requires a reviewed manifest containing the exact current projection revision,
the exact active key/event/value-digest set, and explicit retirements naming
their reviewed event and value digest. It never retires an omitted key. A late
key or changed head requires reload and review before any append. The command
computes the concrete attach/successor disposition against a verified remote
head and rereads the complete comment stream before reporting success.

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
second write, and fails closed on ambiguous or malformed remote state. This is
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

   Promote each material event into the issue graph, then mark it processed.
   A repeated event ID is one event. An unprocessed event is evidence that the
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

After triage, post the remote processed marker with `process`; use disposition
`promoted`, `superseded`, or `no-material-delta`. Never mark an event processed
before the corresponding Linear mutation succeeds when one is needed.

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

## Abrupt termination

Native transcript resume is useful but not cross-machine task authority. The
plugin installs no capture hook, so turns after the last remote acknowledgement
are not recoverable unless the user has separately configured a stable external
capture integration. With that optional integration, a local unacknowledged row
still cannot be recovered after its source machine disappears. State that
physical limit honestly.

The ingress is an at-least-once transport, not a second task tracker. Remote
consumers deduplicate by `event_id`; Linear holds the promoted business logic.
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
