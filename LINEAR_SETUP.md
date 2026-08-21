# Linear setup

Agent Workstream uses ordinary Linear issues and comments. Basic use does not
require custom fields, webhooks, workflow changes, or a dedicated workspace.

## 1. Choose the route

Choose one existing Linear workspace, team, and project for the workstream.
The plugin records their immutable IDs so similarly named projects and teams do
not collide.

For local or personal use, create a personal API key in Linear under **Settings
→ Security & access → Personal API keys**. Keep the key outside the repository
and expose it to the agent process through your shell or secret manager:

```sh
export LINEAR_API_KEY="..."
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

The declaration is currently a checked preflight surface. Until live adapters
consume it directly, include the same exact IDs in the request that starts the
workstream.

## 3. Required access

The key must be able to read teams, projects, issues, and comments and to create
or update the issues and comments used by the workstream. Do not grant
administrative access for this workflow.

Treat `.workstream.json` as non-secret configuration and `LINEAR_API_KEY` as a
secret. If `linear inspect` cannot see the intended route, fix the key's
workspace access before starting a workstream.

See Linear's official [GraphQL and authentication documentation](https://linear.app/developers/graphql)
and [OAuth documentation](https://linear.app/developers/oauth-2-0-authentication)
for provider details.
