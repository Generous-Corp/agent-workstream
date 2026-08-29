# Install

The marketplace name is `generous-workstream`; the plugin name is `workstream`.

## Codex

```sh
codex plugin marketplace add Generous-Corp/agent-workstream --ref main
codex plugin add workstream@generous-workstream
```

If your Git setup prefers SSH, use:

```sh
codex plugin marketplace add git@github.com:Generous-Corp/agent-workstream.git --ref main
codex plugin add workstream@generous-workstream
```

Start a new Codex thread after installation. To update or remove it:

```sh
codex plugin marketplace upgrade generous-workstream
codex plugin add workstream@generous-workstream
codex plugin remove workstream@generous-workstream
codex plugin marketplace remove generous-workstream
```

## Claude Code

```sh
claude plugin marketplace add Generous-Corp/agent-workstream
claude plugin install workstream@generous-workstream
```

Use `/workstream:workstream-resume GEN-123` for an existing handle,
`/workstream:workstream-ledger` for lifecycle work, or let Claude select the
skill. To update or remove it:

```sh
claude plugin marketplace update generous-workstream
claude plugin update workstream@generous-workstream
claude plugin uninstall workstream@generous-workstream
claude plugin marketplace remove generous-workstream
```

## Local development

```sh
codex plugin marketplace add /absolute/path/to/agent-workstream
codex plugin add workstream@generous-workstream
claude --plugin-dir /absolute/path/to/agent-workstream/plugins/workstream
```

Optionally validate a static project declaration in `.workstream.json`:

```sh
plugins/workstream/bin/workstreamctl config validate .workstream.json
```

For the one-time authentication and route-discovery steps, follow
[LINEAR_SETUP.md](LINEAR_SETUP.md).

Live Linear inspection, resume, event, and checkpoint adapters automatically
consume this file from the exact repository root. Initial graph callers can use
`LinearGraphQLTransport.from_config`; conflicting explicit routes fail closed.
The file may be checked in because it contains identifiers and acceptance
commands, not credentials. See
[`workstream.config.schema.json`](plugins/workstream/workstream.config.schema.json).
Runtime credentials stay outside the repository: a protected
`~/.config/agent-workstream/linear.token`, `LINEAR_API_KEY_FILE`, or
`LINEAR_API_KEY` for authenticated Linear operations, plus normal Git/hosting
credentials for private plans and repositories. The optional ingress transport is inactive unless a
separately managed stable capture integration invokes it. Operating
`configure`, `capture`, `bind`, `unbind`, `process`, or `flush` can write local
state and private GitHub issues or comments; read the skill's ingress reference
before use.

Neither plugin silently installs hooks, services, MCP servers, monitors, or
background workers. Installing it does not mutate Linear or GitHub. See
[BOUNDARIES.md](BOUNDARIES.md) for the rationale and planned optional companion
layers.

## Exact-version updates

For one host, update both integrations and emit an immutable verification
receipt with:

```sh
python3 scripts/workstream_plugin_manager.py update \
  --expected-commit <full-main-commit> \
  --expected-version <plugin-version> \
  --host-id <stable-machine-name> \
  --source-root <durable-clean-exact-checkout> \
  --codex-home <absolute-codex-home> \
  --claude-config-dir <absolute-claude-config-dir> \
  --skill-mirror-root ~/.agents/skills
```

The command uses the official Codex and Claude marketplace operations, then
refuses unless the marketplace Git head, manifest version, enabled installation,
and installed-tree digest all match. `doctor` performs the same verification
without changing anything. `--skill-mirror-root` is optional and has no
default; use it only when a shared/global skill directory could otherwise
shadow the plugin. It transactionally synchronizes only the plugin-owned skill
directories, records versioned ownership, preserves unrelated skills, and
includes exact digests in the receipt. Mirror mode requires both clients: it
publishes only after both verify, so a client failure preserves the last-good
mirror. Removed skills are retired only while their prior owned digest remains
unchanged.

```sh
python3 scripts/workstream_plugin_manager.py doctor \
  --expected-commit <full-main-commit> \
  --expected-version <plugin-version> \
  --host-id <stable-machine-name> \
  --source-root <durable-clean-exact-checkout> \
  --codex-home <absolute-codex-home> \
  --claude-config-dir <absolute-claude-config-dir> \
  --skill-mirror-root ~/.agents/skills
```

The source checkout must be durable because the clients retain its local path.
The updater uses a per-host lock and per-client recovery journal, skips clients
that already verify exactly, and emits partial receipts when another client
fails. A changed Codex installation must also survive two delayed readbacks;
if a live older Codex process rematerializes stale plugin cache state, the
update refuses instead of publishing a false-success receipt. Fleet controllers
such as Shipyard should run this one-host command at
an exact approved commit, collect its JSON receipt, stage rollout, and retain
explicit offline/failed hosts for catch-up. The updater does not schedule itself
and does not hold fleet authority.
