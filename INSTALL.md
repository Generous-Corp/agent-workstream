# Install

The repository is private. Authenticate Git access to `Generous-Corp` before
installing. The marketplace name is `generous-workstream`; the plugin name is
`workstream`.

## Codex

```sh
codex plugin marketplace add Generous-Corp/agent-workstream --ref main
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

Validate project routing with a checked-in or local `.workstream.json`:

```sh
plugins/workstream/bin/workstreamctl config validate .workstream.json
```

The file contains identifiers and routing, not credentials. See
[`workstream.config.schema.json`](plugins/workstream/workstream.config.schema.json).
Runtime credentials stay outside the repository: `LINEAR_API_KEY` for the
authenticated Linear operations and normal Git/hosting credentials for private
plans and repositories. The optional ingress transport requires an explicitly
configured private repository and either existing GitHub CLI authentication or
`WORKSTREAM_INGRESS_TOKEN_FILE`; it has no personal or Pulp-specific default.

Neither plugin installs hooks, services, MCP servers, monitors, or background
workers. Installing it does not mutate Linear or GitHub.
