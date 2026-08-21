# Install

The repository is private. Authenticate Git access to `Generous-Corp` before
installing. The marketplace name is `generous-workstream`; the plugin name is
`workstream`.

## Codex

```sh
codex plugin marketplace add Generous-Corp/agent-workstream --ref main
codex plugin add workstream@generous-workstream
```

If the owner/repository form cannot authenticate over HTTPS, use SSH:

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

Use `/workstream:workstream-ledger` or let Claude select the skill. To update or
remove it:

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

The file is a preflight declaration only; current live adapters do not consume
it for routing. It may be checked in because it contains identifiers and
acceptance commands, not credentials. See
[`workstream.config.schema.json`](plugins/workstream/workstream.config.schema.json).
Runtime credentials stay outside the repository: `LINEAR_API_KEY` for the
authenticated Linear operations and normal Git/hosting credentials for private
plans and repositories. The optional ingress transport is inactive unless a
separately managed stable capture integration invokes it. Operating
`configure`, `capture`, `bind`, `unbind`, `process`, or `flush` can write local
state and private GitHub issues or comments; read the skill's ingress reference
before use.

Neither plugin installs hooks, services, MCP servers, monitors, or background
workers. Installing it does not mutate Linear or GitHub.
