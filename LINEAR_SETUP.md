# Linear setup

Agent Workstream uses ordinary Linear issues and comments. Basic use does not
require custom fields, webhooks, workflow changes, or a dedicated workspace.

## 1. Choose the route

Choose one existing Linear workspace, team, and project for the workstream.
The plugin records their immutable IDs so similarly named projects and teams do
not collide.

For local or personal use, create a personal API key in Linear under **Settings
→ Security & access → Personal API keys**. Keep the key outside the repository.
For unattended use, install it once as a private file:

```sh
mkdir -p ~/.config/agent-workstream
chmod 700 ~/.config/agent-workstream
# Write the token without exposing it in shell history, then:
chmod 600 ~/.config/agent-workstream/linear.token
```

The plugin reads that file automatically. Alternatively, `LINEAR_API_KEY`
remains the highest-precedence option for an existing secret manager:

```sh
export LINEAR_API_KEY="..."
```

Or `LINEAR_API_KEY_FILE` can select another protected file:

```sh
export LINEAR_API_KEY_FILE="/path/to/protected/linear.token"
```

The current local plugin uses a personal API key. A service offered to multiple
users should add Linear OAuth rather than asking users to share personal keys.

## 2. Discover and verify the IDs

From a checkout of this repository, run the read-only inspection command:

```sh
plugins/workstream/bin/workstreamctl linear inspect
```

It lists the authenticated identity and the visible workspace, team, and
project IDs. It never prints the API key. After choosing a team and project,
verify that they are associated:

```sh
plugins/workstream/bin/workstreamctl linear inspect \
  --team-id replace-with-linear-team-id \
  --project-id replace-with-linear-project-id
```

Copy the resulting workspace, team, and project IDs into `.workstream.json`
using [examples/workstream.json](examples/workstream.json), then validate it:

```sh
plugins/workstream/bin/workstreamctl config validate .workstream.json
```

The repository-root declaration is the routing authority. Live inspection and
resume commands load it automatically; explicit route arguments must match it.
Set `WORKSTREAM_CONFIG` or pass `--config` only when the declaration deliberately
lives somewhere else.

## 3. Required access

The key must be able to read teams, projects, issues, and comments and to create
or update the issues and comments used by the workstream. Initial `intake`
specifically requires issue-read and issue-create access; it supplies
deterministic issue IDs and verifies the complete route after creation. Do not
grant administrative access for this workflow.

Treat `.workstream.json` as non-secret configuration and every Linear token as
a secret. If `linear inspect` cannot see the intended route, fix the key's
workspace access before starting a workstream.

See Linear's official [GraphQL and authentication documentation](https://linear.app/developers/graphql)
and [OAuth documentation](https://linear.app/developers/oauth-2-0-authentication)
for provider details.
