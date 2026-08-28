# Architecture boundary

Agent Workstream is intentionally a thin, local-first ledger and agent-workflow
plugin. Installing it adds skills and deterministic tools; it does not silently
install hooks, MCP servers, monitors, daemons, or background workers.

## Why keep the plugin thin?

This keeps installation portable across agent clients, avoids ambient execution
or credential access, and leaves Linear as the visible durable record. It also
makes each mutation attributable to an invoked agent rather than an unseen
local service.

An exact-version update may explicitly mirror the plugin-owned skills into a
shared/global skill root when client precedence could shadow the plugin. This
is opt-in, publishes only after both clients verify, preserves unrelated
skills, and is a verified local copy—not another authority or a background
updater.

The tradeoff is that the plugin cannot wake itself, continuously reconcile live
systems, deliver notifications, or accept requests while no agent is running.
Session and landing adapters can supply live facts, but they do not own the
workstream.

## Planned direction

We plan to explore optional companion layers for managed prompt ingress, a
Discord/Harbormaster conversation plane, and hosted or self-hosted coordination.
They should be explicit to install, independently replaceable, and fail without
corrupting the Linear ledger. The base plugin should remain useful without any
always-on service. See the [future roadmap](FUTURE.md).
