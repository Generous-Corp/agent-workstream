# Future integrations

## Optional Harbormaster notifications and ingress

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
