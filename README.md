# agy-trello: Antigravity Trello Sidecar Integration

An Antigravity sidecar plugin that acts as an automated webhook receiver. It intercepts Trello card actions and triggers agent execution cycles via the Antigravity CLI (`agy`).

It supports three distinct behavioral modes depending on the context of the incoming Trello trigger, using semantic classification:
1. **PLANNER (Ready for Spec)**: Leverages `Gemini 3.1 Pro (High)` to analyze requirements, write specifications, create & relate Frontend (FE) and Backend (BE) GitHub issues, link them back to Trello, and update board statuses (e.g. swap label from 'Ready for Spec' to 'Ready for Implementation').
2. **INVESTIGATOR**: Leverages `Gemini 3.1 Pro (High)` and the specialized `grill-me` skill (`~/.agents/skills/grill-me/SKILL.md`) to run an interactive interview on requirements. Responses are simplified, non-technical, and brief.
3. **GENERAL (Conversational)**: Leverages `Gemini 3.5 Flash (Medium)` to handle general questions or discussion comments on cards.

All webhook agent comments are signed with `- Love Maeve`.

---

## Architecture

```
                       +-------------------+
                       |    Trello Board   |
                       +---------+---------+
                                 | (Webhook Event)
                                 v
                       +---------+---------+
                       | Tailscale Funnel  |
                       +---------+---------+
                                 | (Local HTTP Proxy)
                                 v
                       +---------+---------+
                       |   server.py       | (trello-webhook.service)
                       +---------+---------+
                                 |
           +---------------------+---------------------+
           |                     |                     |
           v                     v                     v
   [ PLANNER ]            [ INVESTIGATOR ]        [ GENERAL ]
   (Spec & Git Issues)    (grill-me Interview)    (General Q&A)
           |                     |                     |
           +---------------------+---------------------+
                                 |
                                 v
                       +---------+---------+
                       |  agy CLI Binary   |
                       +-------------------+
```

---

## Configuration & Environment

The sidecar reads environment variables securely to perform actions and authenticate webhook requests.

### Local `.env` Support (Highest Priority)
You can place a `.env` file inside the `sidecars/trello-webhook-receiver/` directory (or the directory where the service is started). Variables defined here take precedence and overwrite variables loaded from systemd environments or `~/.zshrc`.
- `TRELLO_AGENT_SIGNATURE_NAME`: Configure the name used by the agent to sign its Trello comments (e.g. `TRELLO_AGENT_SIGNATURE_NAME=Maeve`). Defaults to `Agy`.

### Trello Authentication
To communicate with Trello, the service automatically injects the following keys from `~/.zshrc` when running under systemd:
- `TRELLO_API_KEY`
- `TRELLO_API_TOKEN`
- `TRELLO_SECRET` / `TRELLO_TOKEN`

### Webhook & Workspace Rules
These can be configured in your `.env` or inside the systemd service file:
- `TRELLO_WEBHOOK_TOKEN`: A secure token to authenticate incoming HTTP POST payloads. Must be provided in the headers (`X-Auth-Token` or `Authorization`).
- `TRELLO_AGENT_WORKSPACES`: A comma-separated list of paths pointing to the target directories. The agent is pinned to these directories during execution. Example:
  ```bash
  TRELLO_AGENT_WORKSPACES=/home/ubuntu/remote-hunter/remote-hunter-backend,/home/ubuntu/remote-hunter/remote-hunter-frontend
  ```

### Runaway Conversation Controls
The receiver includes deterministic guardrails to prevent bot-to-bot loops and duplicate work:
- `TRELLO_AGENT_TRELLO_USERNAME`: Trello username for the account this sidecar posts as. Used to avoid self-mention loops. No source-level default is provided because deployments may post through different human or automation accounts.
- `TRELLO_SUPPRESSED_TRIGGER_USERNAMES`: Comma-separated Trello usernames that should never trigger an agent run. Defaults to `trello,butler`. Add other automation/bot accounts here as they are identified. Do not add the acting account if that same account can also leave intentional human comments.
- `TRELLO_SUPPRESSED_COMMENT_REGEX`: Regex for comment bodies that should never trigger an agent run. Defaults to the configured `- Love <signature>` and legacy planner/investigator signatures.
- `TRELLO_NEVER_MENTION_USERNAMES`: Comma-separated Trello usernames that the helper must never @-mention in agent-authored comments. The helper rewrites those mentions to plain text before posting.
- `TRELLO_MENTION_REPLACEMENTS_JSON`: Optional JSON object mapping Trello usernames to plain-text replacements, for example `{"owner_username":"Owner Name"}`.
- `TRELLO_STAKEHOLDER_CONTEXT_FILE`: Optional path to a local JSON roster. Defaults to `~/.gemini/antigravity-cli/trello_stakeholders.json` when present.
- `TRELLO_STAKEHOLDER_CONTEXT_JSON`: Optional inline JSON roster for managed deployments. Prefer the file path for local development.
- `TRELLO_POST_ACK_COMMENTS`: Whether to post an immediate "Got it" acknowledgement comment before the agent finishes. Defaults to `false` because ack comments can wake other board automations/bots.
- `TRELLO_TRIGGER_COOLDOWN_SECONDS`: Suppresses repeated identical triggers on the same card inside this window. Defaults to `300`.
- `TRELLO_MAX_RECENT_TRIGGER_IDS`: Number of Trello action IDs remembered for replay suppression. Defaults to `500`.

Runtime trigger state is persisted locally under:
`~/.gemini/antigravity-cli/trello_sidecar_state.json`

The sidecar also keeps a single queued/running agent slot per Trello card. If several events arrive while a card is already being processed, only the first is accepted and the rest are ignored.

Example stakeholder context:

```json
{
  "users": [
    {
      "trello_username": "product_owner",
      "display_name": "Product Owner",
      "role": "owner",
      "authority": "final decision maker",
      "preferred_address": "Product Owner",
      "mention_policy": "never_at_mention",
      "tone": "brief, product-facing",
      "notes": "Tagging this account may trigger board automation."
    },
    {
      "trello_username": "designer_username",
      "display_name": "Designer",
      "role": "designer",
      "authority": "design decisions",
      "preferred_address": "Designer",
      "mention_policy": "direct_reply_only",
      "tone": "design-facing, nontechnical"
    }
  ],
  "rules": [
    "Treat support reports as close to customer experience.",
    "Ask product/design questions before deep code investigation."
  ]
}
```

---

## Installation & Deployment

### 1. Configure the systemd service
Copy the template file to your user systemd config directory:
```bash
mkdir -p ~/.config/systemd/user/
cp trello-webhook.service ~/.config/systemd/user/
```

Open `~/.config/systemd/user/trello-webhook.service` and configure the following variables:
- `TRELLO_WEBHOOK_TOKEN`: Set a secure secret key.
- `TRELLO_AGENT_WORKSPACES`: Set paths to your frontend and backend working directories.
- Ensure the path in `ExecStart` points to your actual `server.py` location.

### 2. Start the Service
Reload systemd user daemon and start/enable the service:
```bash
systemctl --user daemon-reload
systemctl --user enable trello-webhook.service
systemctl --user start trello-webhook.service
```

Verify it is running:
```bash
systemctl --user status trello-webhook.service
```

### 3. Tailscale Funnel (Webhook Access)
The receiver automatically attempts to start a **Tailscale Funnel** on port `8454` to expose the local HTTP endpoint to the internet securely. To verify the public URL, run:
```bash
tailscale status
```
Or check the systemd service logs:
```bash
journalctl --user -u trello-webhook.service -f
```

---

## Session Tracking & Resuming Conversations

Conversation history for each Trello card is persisted locally under:
`~/.gemini/antigravity-cli/trello_sessions.json`

This file maps the Trello card ID to the Antigravity conversation UUID.
- If a card has been previously interacted with, the sidecar automatically resumes the same session by passing the `--conversation <UUID>` flag to the `agy` CLI.
- The webhook receiver parses both short card IDs (from URLs) and long card IDs (`cardidLong` / `cardIdLong`) and automatically resolves mappings to the active thread context.
- The receiver checks all known aliases for a card (long ID, short Trello URL ID, and card title) before starting a new conversation. It also injects a compact summary of prior files under `~/.gemini/antigravity-cli/brain/<conversation-id>/` so the agent can see earlier specs, issue bodies, and decisions tied to that card.

---

## Duplicate GitHub Work Prevention

Before a **PLANNER / Ready for Spec** run, the sidecar searches the configured workspace GitHub repositories for likely existing issues and PRs using:
- The originating Trello card URL.
- The Trello short card ID.
- Normalized card title keywords.

Those results are injected into the agent prompt as a mandatory preflight section. The planner is instructed to update or link existing GitHub issues/PRs instead of creating duplicates, and to stop for human confirmation when duplicate status is unclear.

---

## Audience & Process Automation Rules

- Trello comments are kept short and product-facing for PMs, designers, QA, and reporters.
- Technical implementation detail belongs in GitHub issues, not Trello comments.
- Investigator mode does only light code grounding while product/design questions are unresolved; deep code dives and Codex review are reserved for planner mode after requirements settle.
- If related PRs are already merged/deployed and new changes are requested, the sidecar treats them as follow-up work and links back to the original card for context.
- Deployment-specific stakeholder names, roles, usernames, and mention policies belong in local stakeholder context, not committed source.

---

## Public Release Privacy

Before making this repository public, scan and scrub both the working tree and git history for private stakeholder names, usernames, project paths, board/card URLs, and secrets. Deleting private data from current files is not enough for an open source release; plan a deliberate history rewrite and fresh verification pass.

---

## Security Verification Rules

The prompt injected into the agent contains strict safety directives:
- **No credential leaks**: The agent is explicitly forbidden from printing, commenting, or revealing any credentials, API keys, secrets, tokens, or passwords under any circumstances.
- This applies to Trello credentials, database passwords (like `db_reader`), `.env` secrets, or other configurations. Only the *presence* of these configurations may be verified.
