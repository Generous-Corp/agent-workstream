---
name: workstream-ledger
description: Preserve evolving project goals, tasks, decisions, dependencies, PR state, and next actions in a durable Linear-backed workstream across Codex/Claude sessions and machines. Use when executing a planning file, starting substantive or PR-bound work, adding or changing requirements during conversation, cancelling or superseding earlier decisions, handing a PR to Shipyard, detecting stale work, recovering after quota/session loss, or preparing/restoring a cmux successor session.
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

The live Linear graph transport currently neither assigns a project nor reads
or writes these relations. Treat that as a transport gap: validate and preserve
the logical scope, but do not claim live routing/relation proof until a
paginated, authenticated read-back-verified adapter exists. Until then it emits
`transport_unimplemented`. The deterministic validator rejects missing or
mismatched receipts; the transport remains responsible for their authenticity.

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
  ./PLAN.md --identity https://github.com/org/private-plans/blob/main/PLAN.md
```

Pasted Markdown works too. First commit the exact text to its durable planning
repository, then run the committed checkout path with its canonical URL as the
identity. If persistence must follow intake, exact bytes may be piped on stdin
with `-`; supply `--identity` as soon as a durable reference exists. An omitted
identity makes stdin content-addressed and is only a provisional snapshot.

The output's `source.sha256`/`root.plan_revision` is the plan revision to record
on the single Linear root. This helper is intentionally side-effect free and
sets `graph_review_required=true`: Markdown structure can produce deterministic
candidates, but cannot prove which sections are independently actionable. The
active agent must review completeness before creating the root and accepted
children. The current GraphQL transport can create an initial graph and treats
an exact same-revision repeat with all accepted children present as a zero-write
no-op. It cannot serialize concurrent first creation or safely rewrite an
existing graph: those paths fail `remote_cas_unavailable` until a remote
serialization/CAS authority exists. Never infer a repository, worktree, or
Linear project from the Markdown's current directory.

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
exact root issue identifier. Do not use a locally cached comment subset as the
reducer input.

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

This is currently a deterministic logical contract only. The generic material
delta transport remains unchanged. A future adapter must connect choice events
to fully paginated Linear comments and resume readback before cross-machine
recovery can be claimed.

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

The current paginated Linear transport obtains next actions from durable root
and child descriptions without synthesizing them. It still returns empty
decisions and provenance and does not fetch typed choices, scope, relations,
evidence contracts, owner, history, execution identity, or live GitHub/Shipyard
truth. Therefore its output is a validated bounded snapshot, not yet a physical
cross-machine recovery proof. Resume must recover and reduce every durable
choice event, then reconcile its plan revision, repository scope, and exact
head before implementation. Resume emits `transport_unimplemented` for each
unfetched surface; closure treats every such marker as a blocker. Absence is a
named pilot gate, not evidence that no choices exist and never closure-ready.

### Linear graph operations

`scripts/workstream_graph.py` converts an intake payload plus a reviewed set of
candidate keys into deterministic root/child operations and refuses child
creation before explicit review. Stable keys make sequential comparison and
same-revision no-op detection deterministic; they are not a server-side unique
constraint. The current authenticated transport rejects duplicate observed
keys and all existing-graph mutations that would require remote CAS. Concurrent
first creation remains unproven and must not be described as idempotent.

### Material-boundary checkpoint schema

`scripts/workstream_checkpoint.py` defines deterministic checkpoint identity,
required execution/worktree fields, remote-ack validation, plan-drift checks,
and complete predecessor-chain recovery. `scripts/workstream_linear_checkpoints.py`
persists that schema as a distinct immutable marker over the same authenticated,
fully paginated Linear comment boundary as material-delta events. It derives an
acknowledgement only after readback, replays an already observed event without a
second write, and fails closed on ambiguous or malformed remote state. This is
logical transport proof: do not call the deterministic fake-client tests a live
Linear, second-machine, process-death, or deletion-resistance canary.

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
3. On recovery, read the full nonterminal issue graph, dependencies, decisions,
   comments, plan link/revision, and next action. Then query live GitHub and
   Shipyard state; neither old comments nor local state are PR truth.
4. Report stale contradictions before implementation:
   - open issue whose PR is merged/closed or whose head changed;
   - waiting issue with no blocker/owner/review date;
   - completed parent with nonterminal children;
   - cancelled decision still present in acceptance criteria;
   - PR without a workstream, exact head, provenance, or landing owner.
5. Recover turns that arrived after the last structured checkpoint:

   ```sh
   python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" flush
   python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" \
     recover --workstream ABC-123
   ```

   Promote each material event into the issue graph, then mark it processed.
   A repeated event ID is one event. An unprocessed event is evidence that the
   next agent must triage it, not evidence that the request was accepted.

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
| Shipyard accepts landing | Attach the exact-head controller receipt |
| PR merged | Close only acceptance that actually passed; retain follow-ups |
| No material delta | Do not write to Linear |

After creating the first durable issue for a session that began unbound, bind
the already-captured turns using the exact current cmux surface:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" \
  bind --workstream ABC-123 --context-url https://linear.app/example/issue/ABC-123/example \
  --surface "$CMUX_SURFACE_ID"
```

For a legacy process with no trusted cmux surface metadata, use the exact
provider session ID from `recover`; never bind by repository cwd. Multiple tabs
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
coordinate, why, completion gate, PR, repository-qualified head, Shipyard
receipt, evidence, current blockers, and next action.

## Checkpoint before continuation

Before invoking `cmux-continue-session`:

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
6. Pass `--workstream`, `--context-url`, and `--context-updated-at` to the cmux
   continuation script. Include `--plan-url`, `--head`, and `--pr-url` when they
   exist.
7. Require the new session to reread the durable graph and live PR state and
   acknowledge the restored objective, decisions, open items, head, and next
   action. Do not close the source beforehand.

## Abrupt termination

Native transcript resume is useful but not cross-machine task authority. If a
user turn lacks a remote processed acknowledgement, treat it as an open
obligation during recovery. The ingress hook synchronously writes a private
SQLite outbox before trying the private GitHub remote. GitHub downtime leaves a
local unacknowledged row for `flush`; if the entire source machine is offline
before remote acknowledgement, another machine cannot recover that row. State
that physical limit honestly.

The ingress is an at-least-once transport, not a second task tracker. Remote
consumers deduplicate by `event_id`; Linear holds the promoted business logic.
Local remote-acknowledged rows rotate after 30 days, remote issues rotate by
machine/month, prompts are capped at 16 KiB, and credential-like values are
redacted. See [durable ingress](references/durable-ingress.md).

Never store credentials, account identities, private filesystem paths, or raw
transcripts in public PR metadata.

Future backup work may add a read-only private flat-file snapshot, but it must
not become an import/sync path or second authority.
