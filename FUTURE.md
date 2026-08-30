# Future integrations

This file is the canonical appendix for ideas intentionally deferred beyond the
current acceptance plan. An entry here is not implemented behavior, landing
authority, or permission to expand an active workstream. Current P0 recovery and
closure gates remain in the tracked plan and must not be moved here merely to
close the pilot.

## Roadmap at a glance

- **Conversation plane:** actionable Discord notifications, read-only status,
  and safely typed workstream requests through Harbormaster.
- **Managed ingress:** durable opt-in capture of requests across agent clients.
- **Control plane:** optional hosted or self-hosted scheduling, health, and
  centralized visibility without replacing Linear or Shipyard authority.
- **Portability and archives:** more planning/source providers plus deterministic
  export after the live schema stabilizes.
- **Fleet expansion:** more profiles, platforms, clients, and a
  provider-neutral controller API.

## Conversation plane: Harbormaster and Discord

A future adapter may project durable workstream lifecycle events into Discord
through Harbormaster. It should be disabled by default and configurable by:

- event: human-needed blocker, landing failure, stale handoff, closure refusal,
  started, PR ready or landed, and completed;
- destination: server/channel plus optional role or user mentions;
- delivery: immediate urgency or a periodic active-workstream digest; and
- noise controls: deduplication, unchanged-state suppression, and rate limits.

Routine checkpoints and retries should not notify. Every notification should
link back to the authoritative Linear workstream; Discord delivery must never
be required for resume, state transitions, or closure.

A later Discord command may submit a plan URL or workstream token. Harbormaster
should durably acknowledge and deduplicate that request, but a separate trusted
agent/controller must validate configuration, review the child graph, mutate
Linear, and return the resulting token. Discord retries must never create a
second workstream.

Harbormaster may also become a natural-language router over all tracked
workstreams: answer status/blocker/history/location questions from Linear plus
live repository and landing state, or submit typed `start`, `continue`, and
`stop` requests to a trusted controller. Natural-language interpretation must
not become direct execution authority. Read-only intents may run immediately;
mutating intents require an idempotency key, durable receipt, bounded typed
payload, and risk-appropriate confirmation. No path may translate free-form
Discord text directly into a shell command.

## Managed prompt ingress

A future opt-in package may install and maintain stable prompt-capture hooks for
Codex, Claude, and other agents. It must keep credentials outside public
repositories, acknowledge capture durably before local deletion, support
at-least-once replay and explicit binding to a reviewed workstream, and remain
inactive by default. Captured text is evidence of a user turn, not authority to
classify, mutate, execute, or close a workstream.

The portable plugin already provides the reviewed raw-capture-to-Linear
promotion protocol and crash replay. This roadmap item is the managed hook and
deployment layer that feeds it, not a second promotion or ledger authority.

A later design may add an immutable or signed receipt for reviewed
`no-material-delta` and `superseded` classifications. Do not treat an editable
GitHub comment, current login, user ID, or unchanged timestamp as that receipt,
and do not introduce signing keys or key management until the authority,
rotation, revocation, and recovery contract is specified and reviewed. Until
then only an exact immutable Linear material event plus verified receipt may
close a captured turn.

## Hosted control plane

Agent Workstream may eventually gain an optional hosted or self-hosted control
plane for scheduling adapters, fleet health, and centralized status. Linear
should remain the portable execution ledger, while Shipyard or an equivalent
controller remains the authority for exact-head execution and landing. The
service must be replaceable: installing the plugin alone should continue to
provide local start, status, resume, and closure contracts without requiring a
vendor daemon.

## Cross-issue serialization

Linear comment creation cannot atomically compare the root projection frontier
with material comments on every nonterminal child. The current protocol
validates the complete child-aware candidate, rechecks that frontier immediately
before each projection append, and refuses authority if the final frontier
changed. An append in the remaining API check/use gap can therefore leave one
inert projection comment, but cannot return valid authority.

A future protocol may eliminate even that inert write with a cooperative
multi-issue reservation respected by every root and child material writer, or
with a provider-native atomic primitive. Do not claim arbitrary-interleaving
zero-write behavior until the reservation, crash recovery, stale-owner expiry,
and mixed-client compatibility are implemented and physically tested.

## Provider and repository portability

Future adapters should support additional source-control and planning providers
without inferring identity from a local path or repository name. Durable
identity must include provider, host, owner or organization, repository, and
workstream ID. Projects remain isolated by explicit Linear workspace, team, and
project IDs; cross-project or cross-repository dependencies use typed links
rather than shared numbering or naming conventions.

The portable configuration may later move into a conventional repository-local
directory when more than one config artifact is justified. Migration must keep
the current `.workstream.json` route unambiguous and avoid introducing a second
authority.

## Export and archival

After the live schema and pilot workflow stabilize, add a deterministic export
to logical flat files and/or SQLite for backup, audit, offline inspection, and
provider migration. Preserve immutable event and decision history and derive
current views; do not rewrite history into a second mutable project authority.
Import, bidirectional sync, and disaster recovery should be designed only after
the export format has real compatibility fixtures.

## Additional client and fleet automation

Future fleet support may enumerate multiple named Codex/Claude profiles per
host, support non-macOS workers, and expose a provider-neutral controller API.
Each target still requires explicit identity and an independent exact-version
receipt. A global agent may route natural-language requests to that API, but it
must not collapse profile, machine, repository, or workstream identities into a
single ambient default.

For hostile same-user environments, harden skill-mirror publication further by
performing every target operation relative to the already verified directory
descriptor (`openat`-style), eliminating the remaining check/use race if another
process swaps the mirror root during an update. Current ancestry and inode
checks cover accidental redirection; this is deferred adversarial hardening.
