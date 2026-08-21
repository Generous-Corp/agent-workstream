# Durable prompt ingress

This is a small at-least-once safety net for the interval between a user prompt
arriving and an agent updating Linear. It is deliberately not a transcript
store and does not decide whether a request is material.

## Authority and data flow

1. Codex or Claude invokes `capture` on `UserPromptSubmit` before model work.
2. `capture` inserts an idempotent event into a mode-0600 SQLite outbox.
3. It synchronously attempts a bounded upload to a private, monthly GitHub issue.
4. A recovering agent reads remote capture, bind, and processed markers,
   deduplicates by `event_id`, and promotes material deltas into Linear.
5. Only after the Linear mutation succeeds does the agent post a processed
   marker. `no-material-delta` and `superseded` remain explicit history.

GitHub is transport durability; Linear is business-logic authority. cmux owns
session topology and native transcripts. Shipyard owns an exact PR head after
handoff. None substitutes for another.

## Setup

Resolve the directory containing the skill's `SKILL.md` as
`WORKSTREAM_SKILL_ROOT`, then configure an explicitly chosen private transport
repository:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" configure \
  --repo your-org/private-workstream-ingress --machine build-mac
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" status
```

`configure` creates or reuses a private issue named
`[Workstream ingress] <machine> YYYY-MM`. Run it again at month rollover; a
later scheduler may automate that only after a bounded failure/recovery proof.
The Workstream plugin itself installs no hooks or background process.

## Persisted bindings (why an unbound backlog stops growing)

`bind` records the identity in a `bindings` table, not just on the rows that
already exist. Capture consults it, so a session that binds once binds its
**later** turns automatically. Without that, every turn after the bind was
captured unbound again and nothing revisited it — which is how 36 legacy
events became 315.

Only two identity kinds are storable, and the schema enforces it with a
`CHECK (kind IN ('session', 'surface'))`: an exact provider session, or a cmux
surface. **There is no cwd row and no heuristic fallback.** Several tabs share
one checkout, so a cwd binding would attach turns to whatever workstream ran
there last. An unbound event is a visible gap that `status` counts; a wrongly
bound one is a silent lie that no recovery pass can find.

Precedence at capture: an explicit `WHENCE_WORKSTREAM_ID` wins, because a
caller naming a workstream for *this* turn is a more specific statement than a
binding recorded earlier. A persisted binding only fills a gap.

`unbind` forgets the persisted identity as well as clearing the rows. Without
that, correcting a mistaken binding would silently re-apply the same wrong
workstream on the session's very next turn.

`status` reports the volume, because the gap is only safe if it is visible:

```json
{"unbound_events": 315, "unbound_sessions": 75,
 "oldest_unbound_age_hours": 50.4, "persisted_bindings": 0}
```

Sessions and events are counted separately on purpose — "many sessions, days
old" and "one session, minutes old" call for opposite responses.

## Watching the backlog: `ratchet`

```sh
python3 .../workstream_ingress.py ratchet    # exit 1 = something to act on
```

It reports two numbers that mean different things, and only one of them is an
alert:

- **`unbound_events` is a LEVEL** — history. A known quantity that only a
  triage pass reduces. Alerting on it would be permanently red, and a
  permanently red check gets muted, which is worse than no check because it
  looks like coverage. `status` reports it; `ratchet` does not alert on it.
- **`grew_by` is the growth since the last check.** The baseline is rewritten
  on **every** run, including one that grew, so each interval is judged on its
  own and the check clears once growth stops. A ratchet that held the old
  baseline after an increase would stay red until the whole backlog was
  triaged — the muting failure again.
- **`unbound_with_binding` is an INVARIANT and the sharpest signal.** Capture
  resolves a workstream from the bindings table, so an event whose session or
  surface is already bound can never legitimately be unbound. Any nonzero
  value means capture-time resolution regressed.

Run it on a schedule and alert on a nonzero exit. Note that `grew_by > 0` fires
for a legitimately new unbound session too — an agent that started work without
binding — which is actionable rather than noise, but if it proves noisy in
practice, `unbound_with_binding` is the one to keep.

## Triaging a backlog: read the population when the population is small

Bulk-classifying an unbound backlog is tempting and is where a real request
gets buried. The guard is the read, not the intention.

On 2026-08-17 the 67 single-event sessions were assumed to be mostly machine
notifications and were going to be batched on that basis. Reading all 67
instead showed the premise was wrong in both directions:

- **zero** of them carried a machine tag (`<task-notification>`,
  `<cross-session-message>`), so the prefix heuristic that would have driven the
  batch matched nothing;
- 54 were an agent generation harness and 12 were route probes — genuinely
  no-material-delta;
- **one was a real user request** that had sat unbound for 50 hours and had
  never been actioned.

A 1-in-67 request has good odds of surviving a sample. It does not survive a
full read. When the population is small enough to read — single-event sessions
especially — read it rather than defending a sample size; it is usually the
cheaper option as well as the safer one.

Two rules that follow:

- Classify by what a prompt IS, not by which directory it ran in. cwd was
  useless here: the harness prompts and the user request shared checkouts.
- If a batch turns up one genuine request, stop the batch and escalate it
  rather than continuing. The batch will still be there afterwards; the
  request's context may not be.

The same shape appears again when ACTING on a recovered request, one layer
further out. Triaging the recovered worktree-cleanup request produced a strict,
correct test for "safe to delete": branch merged into `origin/main`, no
uncommitted files, zero commits ahead. Its largest candidate passed every check
and was the working directory of a live recovery-lane sequence.

**Safe-by-content and safe-to-act-on are different predicates, and a repository
can only answer the first.** Git knows what a tree contains; it cannot know that
someone is standing in it. Promote a recovered request into an inventory a human
can decide from — never into an action an agent takes because the data looked
unanimous.

## Recovery and acknowledgement

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" flush
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" recover \
  --workstream ABC-123
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" process \
  --repo your-org/private-workstream-ingress --remote-issue 42 --event wsi_... \
  --disposition promoted --issue GEN-124
```

For a new session whose first prompt arrived before a workstream existed, bind
the current surface after creating the Linear issue:

```sh
python3 "$WORKSTREAM_SKILL_ROOT/scripts/workstream_ingress.py" bind \
  --workstream ABC-123 --context-url https://linear.app/example/issue/ABC-123/example \
  --surface "$CMUX_SURFACE_ID"
```

Binding fails closed without an exact event, provider session, or trusted cmux
surface identity. Repository cwd is deliberately insufficient because several
tabs can share a checkout. Correct a mistaken binding without discarding the
event with `unbind --workstream ABC-123 --session <exact-session-id>`.

## Retention and privacy

- Local remote-acknowledged rows older than 30 days are pruned. Unuploaded rows
  are never silently discarded; a full/offline outbox must be repaired.
- The default local ceiling is 50 MiB. `prune` compacts the SQLite file.
- Remote issues are partitioned by machine and month. The configured remote
  retention is 90 days; destructive remote deletion is intentionally not
  automatic during the pilot.
- Prompts are capped at 16 KiB. OAuth query values, bearer tokens, common API
  key forms, passwords, and private-key blocks are redacted before local or
  remote persistence. Redaction is defense in depth, not permission to paste
  secrets into prompts.
- Raw prompt material never belongs in public PR metadata, Whence provenance,
  Shipyard receipts, or public repositories.

## Credential path (why capture used to fail silently)

The hook fires in whatever shell the agent runs in — `codex exec`, a spawned
subagent, a launchd-started job. Those are non-interactive and do not read an
interactive profile, which produced three distinct failures on M5 between
2026-08-15 and 2026-08-17 that together left 55 rows unacknowledged:

| Recorded message | Real cause | `cause` |
| --- | --- | --- |
| `[Errno 2] No such file or directory: 'gh'` | `/opt/homebrew/bin` absent from PATH | `gh-missing` |
| `API rate limit exceeded for <IP>` | request was **anonymous** — GitHub names an IP only for unauthenticated calls (60/hr, shared by every tool on the machine) | `unauthenticated` |
| `Requires authentication (HTTP 401)` | no usable credential in that shell | `unauthenticated` |

So:

- `gh` is resolved with `shutil.which`, then `GH_SEARCH_PATHS`, then a
  `gh_bin` config key or `WORKSTREAM_INGRESS_GH_BIN`. A miss raises an error
  that names the searched locations instead of a bare `FileNotFoundError`.
- Authentication uses an existing `GH_TOKEN` / `GITHUB_TOKEN`; otherwise a
  **0600** file at `~/.config/workstream/ingress-token` (override with
  `WORKSTREAM_INGRESS_TOKEN_FILE`) is passed to the subprocess.
  A group- or world-readable token file is refused, and no token value is ever
  written to the failure log. `gh`'s own keyring is deliberately not relied on:
  a hook cannot answer a keychain prompt.
- Every failure is classified in `failures.jsonl` under `cause`, and `status`
  reports the tally alongside the resolved `gh_binary` and whether a token file
  is present. The three causes need opposite fixes and their raw messages do
  not say which.

Materialise the token once per machine. The file is the live path; a password
manager may remain the backup:

```sh
mkdir -p ~/.config/workstream && chmod 700 ~/.config/workstream
umask 077; op read "op://Private/<item>/<field>" > ~/.config/workstream/ingress-token
chmod 600 ~/.config/workstream/ingress-token
```

Until that file exists the ingress falls back to whatever `gh` can find on its
own, which is why the backlog drain below matters.

## Backlog drain

A capture whose own upload succeeded has just proved the credential path works,
so it opportunistically retries up to five older unacknowledged rows, oldest
first, stopping at the first failure. A transient outage or one credential-less
shell therefore heals on the next good turn instead of waiting for someone to
notice and run `flush`. That is how 55 rows accumulated across 55 one-shot
sessions before anyone looked.

## Flushing a backlog

`flush` stops at the first refusal — if the remote just refused, the rest of the
backlog will refuse identically and retrying makes it worse. It reports **why**
it stopped in its own output rather than only in `failures.jsonl`:

```json
{"pending_before": 58, "uploaded": 0, "remaining": 58,
 "stopped_because": "github-unavailable",
 "stopped_detail": "gh: No server is currently available ..."}
```

A silent stop was indistinguishable from a flush with nothing to do, which is
the same defect class as a capture that fails without saying so. A clean flush
reports neither field and writes nothing to the failure log, so the signal
stays meaningful.

## Failure semantics

- Remote success is acknowledged locally only after GitHub returns a comment.
- Remote failure never blocks the agent prompt; the local row remains for
  `flush`.
- A crash after remote POST but before local acknowledgement may duplicate a
  comment. Recovery deduplicates by `event_id`.
- If the source machine goes offline before any remote acknowledgement, another
  machine cannot access its local-only row. No software can provide
  cross-machine recovery without a reachable copy; the source flushes when it
  returns.
