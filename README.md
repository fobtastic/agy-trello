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

### Trello Authentication
To communicate with Trello, the service automatically injects the following keys from `~/.zshrc` when running under systemd:
- `TRELLO_API_KEY`
- `TRELLO_API_TOKEN`
- `TRELLO_SECRET` / `TRELLO_TOKEN`

### Webhook & Workspace Rules
These are configured inside the systemd service file:
- `TRELLO_WEBHOOK_TOKEN`: A secure token to authenticate incoming HTTP POST payloads. Must be provided in the headers (`X-Auth-Token` or `Authorization`).
- `TRELLO_AGENT_WORKSPACES`: A comma-separated list of paths pointing to the target directories. The agent is pinned to these directories during execution. Example:
  ```bash
  TRELLO_AGENT_WORKSPACES=/home/ubuntu/remote-hunter/remote-hunter-backend,/home/ubuntu/remote-hunter/remote-hunter-frontend
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

---

## Security Verification Rules

The prompt injected into the agent contains strict safety directives:
- **No credential leaks**: The agent is explicitly forbidden from printing, commenting, or revealing any credentials, API keys, secrets, tokens, or passwords under any circumstances.
- This applies to Trello credentials, database passwords (like `db_reader`), `.env` secrets, or other configurations. Only the *presence* of these configurations may be verified.
