# agy-trello: Antigravity Trello Sidecar Integration

An Antigravity sidecar plugin that acts as an automated webhook receiver. It intercepts Trello card actions and triggers agent execution cycles via the Antigravity CLI (`agy`).

It supports three distinct behavioral modes depending on the context of the incoming Trello trigger, using semantic classification:
1. **PLANNER (Ready for Spec)**: Leverages `gemini-pro-orchestrator` (configurable via `TRELLO_AGENT_MODEL`) to analyze requirements, write specifications, create & relate Frontend (FE) and Backend (BE) GitHub issues, link them back to Trello, and update board statuses (e.g. swap label from 'Ready for Spec' to 'Ready for Implementation').
2. **INVESTIGATOR**: Leverages `gemini-pro-orchestrator` and the specialized `grill-me` skill (`~/.agents/skills/grill-me/SKILL.md`) to run an interactive interview on requirements. Responses are simplified, non-technical, and brief.
3. **GENERAL (Conversational)**: Leverages `gemini-pro-orchestrator` to handle general questions or discussion comments on cards.

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
- `TRELLO_AGENT_MODEL`: Override the `agy --model` value used for all card phases. Defaults to `gemini-pro-orchestrator`.
- `TRELLO_CLASSIFIER_MODEL`: Override the `agy --model` value used for semantic phase classification. Defaults to `TRELLO_AGENT_MODEL`.
- `TRELLO_CODEX_REVIEW_MODEL`: Override the Codex model ID named in the PLANNER review instructions. Defaults to `gpt-6.2-sol`.

### Trello Authentication
To communicate with Trello, the service automatically injects the following keys from `~/.zshrc` when running under systemd:
- `TRELLO_API_KEY`
- `TRELLO_API_TOKEN`
- `TRELLO_SECRET` / `TRELLO_TOKEN`

### Webhook & Workspace Rules
These can be configured in your `.env` or inside the systemd service file:
- `TRELLO_WEBHOOK_TOKEN`: A secure token to authenticate relay POST payloads (`X-Auth-Token` or `Authorization: Bearer <token>`).
- `TRELLO_WEBHOOK_CALLBACK_URL`: Optional public callback URL registered with Trello; when set alongside `TRELLO_SECRET`, direct Trello webhooks signed via `X-Trello-Webhook` (HMAC-SHA1 over raw body + callback URL) are also accepted.
- `TRELLO_CLIENT_IDENTIFIER`: Identifier sent in `X-Trello-Client-Identifier` on sidecar Trello writes and ignored if echoed back in incoming webhook headers. Defaults to `agy-trello-sidecar`.
- `TRELLO_MAX_WEBHOOK_BODY_BYTES`: Maximum accepted webhook request body size in bytes before returning HTTP `413`. Defaults to `1048576` (1 MiB).
- `TRELLO_WEBHOOK_READ_TIMEOUT_SECONDS`: Deadline in seconds for reading the incoming webhook body before returning HTTP `408`. Defaults to `10`.
- `TRELLO_MAX_HTTP_WORKERS`: Maximum concurrent HTTP handler threads before rejecting excess requests with HTTP `503`. Defaults to `16`.
- `TRELLO_MAX_PROMPT_BYTES`: Maximum UTF-8 byte budget for assembled `agy` prompts. Defaults to `96000` (clamped to a minimum of `16384` so required instructions and card identity remain intact).
- `TRELLO_DEFERRED_RETRY_MAX_ATTEMPTS`: Maximum automatic deferred requeues for `READY_FOR_SPEC` triggers while live card link verification is unavailable (`trigger_deferred`). Defaults to `3`.
- `TRELLO_DEFERRED_RETRY_DELAY_SECONDS`: Delay in seconds before requeueing a deferred `READY_FOR_SPEC` trigger. Defaults to `15`.
- `TRELLO_ENABLE_TAILSCALE_FUNNEL`: Set to `true` only if the sidecar itself should launch `tailscale funnel` for public ingress instead of using an existing reverse proxy (such as Apache). Defaults to `false` so existing `tailscale serve` configs are untouched.
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
- `TRELLO_POST_ACK_COMMENTS`: Whether to post a safe "Got it" acknowledgement comment after a trigger is accepted and classified as long-running (`INVESTIGATOR` or `READY_FOR_SPEC`). Defaults to `false`; when enabled, acks are marker-tagged and cooldown-limited.
- `TRELLO_ACK_COOLDOWN_SECONDS`: Minimum seconds between ack comments on the same card. Defaults to `900`.
- `TRELLO_AGENT_COMMENT_MARKER`: Marker that identifies sidecar-authored substantive comments. Defaults to `[agy-sidecar:comment]`.
- `TRELLO_AGENT_ACK_MARKER`: Marker that identifies sidecar acknowledgement comments. Defaults to `[agy-sidecar:ack]`.
- `TRELLO_AGENT_STATUS_MARKER`: Marker that identifies sidecar status-only comments. Defaults to `[agy-sidecar:status]`.
- `TRELLO_AGENT_COMMENT_USER_REGEX`: Regex used to detect likely agent/bot Trello users for novelty gating.
- `TRELLO_TRIGGER_COOLDOWN_SECONDS`: Suppresses repeated identical triggers on the same card inside this window. Defaults to `300`.
- `TRELLO_MAX_RECENT_TRIGGER_IDS`: Number of Trello action IDs remembered for replay suppression. Defaults to `500`.

Runtime trigger state is persisted locally under:
`~/.gemini/antigravity-cli/trello_sidecar_state.json`
Quarantined interrupted runs keep detailed entries (`unreconciled_cards`) up to 500 records while retaining unified per-run replay tombstones (`unreconciled_tombstones`, linking the record ID, any known `action_id`, card-scoped `delivery_keys`, and replay fingerprints together). Reconcile an interrupted run in-process while the sidecar is running via `POST /admin/reconcile` (authenticated with `X-Auth-Token` or `Authorization: Bearer <token>` and JSON body `{"actionId": "..."}`, `{"fingerprint": "..."}`, or `{"recordId": "..."}`), which clears every key on that record atomically under `state_lock`. If an `actionId` or `fingerprint` selector matches more than one quarantined run, `POST /admin/reconcile` returns HTTP `409` with `matchingRecordIds` and leaves all records blocked until you disambiguate with `recordId`. Pre-unified split-index states (`unreconciled_action_ids` and `unreconciled_fingerprints`) whose `unreconciled_cards` detail was already evicted migrate into separate `legacy-action:` and `legacy-fp:` tombstones; pass both known selectors in one `POST /admin/reconcile` request (`{"actionId": "...", "fingerprint": "..."}`) or check `remainingLegacyOrphans` in the response if one remains. Calling `reconcile_quarantined_entry(...)` directly from Python is offline-only: stop `trello-webhook.service` first so two processes cannot race on `trello_sidecar_state.json`. Relays should include Trello `actionId` or a unique delivery key (`deliveryId`, `eventId`, `webhookEventId`, or `X-Delivery-Id` / `X-Idempotency-Key` header, stored independently of refreshed card metadata); without either, replay detection uses `relay_replay_fingerprint` (card, action type, author, comment, list/label/move fields), and migrated headerless rows keep compatibility matching across replays.

Structured sidecar events are appended locally under:
`~/.gemini/antigravity-cli/trello_sidecar_events.jsonl`

Override the path with:
- `TRELLO_SIDECAR_EVENT_LOG_FILE`: Path to the JSONL event log.

The sidecar also keeps a single queued/running agent slot per Trello card. If several events arrive while a card is already being processed, only the first is accepted and the rest are ignored.

Agent-to-agent comments are allowed when they add meaningful new content. Probable agent comments are suppressed only when they are low novelty, such as acknowledgement/status pings, mention-only chatter, repeated wording, or parroting recent comments without a new question, decision, bug detail, repro step, URL/screenshot/mockup, issue/PR link, requirement change, or explicit action request.

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

### 3. Tailscale Funnel (Optional Webhook Access)
If you do not already terminate public HTTPS traffic through a reverse proxy (for example Apache), set `TRELLO_ENABLE_TAILSCALE_FUNNEL=true` so the receiver attempts to start a **Tailscale Funnel** on port `8454`. Otherwise Funnel stays disabled so existing `tailscale serve` listeners are unaffected. To verify Tailscale status, run:
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
- New and updated conversation UUIDs are captured from `agy --output-format stream-json` as soon as the `init` event arrives, and if a previously saved conversation was pruned or replaced, the sidecar automatically re-links all immutable ID aliases for that card to the new UUID.
- The webhook receiver parses both short card IDs (from URLs or `card.shortLink`) and long card IDs (`cardidLong` / `cardIdLong`), canonicalizes card IDs before queuing so short and long ID webhooks share one slot, and falls back to card title only when no immutable card ID exists. It also injects a compact summary of prior files under `~/.gemini/antigravity-cli/brain/<conversation-id>/` so the agent can see earlier specs, issue bodies, and decisions tied to that card.

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

## Superpowers Skills

Install the common Superpowers skills for agents that support `skills.sh`:

```bash
npx skills add obra/superpowers -g --skill using-superpowers brainstorming writing-plans systematic-debugging test-driven-development verification-before-completion --copy -y
```

The sidecar prompts tell Agy/Codex to use Superpowers-style discipline when available: brainstorm unclear product work, debug systematically, write structured implementation plans, include TDD-minded acceptance criteria, and verify before declaring completion. Trello comments should still stay product-facing and avoid tool/process ceremony.

---

## Dashboard Direction

The sidecar should stay lightweight while behavior is still evolving. It now emits structured JSONL events that a future dashboard can import without forcing the webhook runner itself to become Laravel immediately.

Current event types include:
- `trigger_accepted`
- `trigger_ignored`
- `trigger_blocked`
- `trigger_deferred`
- `trigger_classified`
- `trigger_requeued`
- `trigger_interrupted_unreconciled`
- `ack_posted`
- `agent_run_started`
- `conversation_linked`
- `agent_run_completed`
- `agent_run_failed`
- `agent_run_error`

Events include compact fields such as card ID/name/link, action ID/type, member username/name, phase, reason, model, conversation ID, run duration, return code, comment hash, and comment length. Full comment bodies, raw payloads, stdout/stderr, and obvious token/key query values are not written to the event log.

---

## Public Release Privacy

Before making this repository public, scan and scrub both the working tree and git history for private stakeholder names, usernames, project paths, board/card URLs, and secrets. Deleting private data from current files is not enough for an open source release; plan a deliberate history rewrite and fresh verification pass.

---

## Security Verification Rules

- **Classifier isolation**: Semantic phase classification runs in an empty temporary working directory with `--sandbox`, a stripped environment (no Trello/GitHub/database credentials), no `--dangerously-skip-permissions`, and immediate process-group termination if any tool step or framing error occurs; any classifier tool attempt aborts the job before responder launch (`trigger_blocked`).
- **Crash recovery safety**: Queued triggers not yet dispatched to the responder re-enqueue automatically on restart (`trigger_requeued`); runs interrupted after responder dispatch move to `unreconciled_cards` (`trigger_interrupted_unreconciled`) rather than replaying external writes automatically.
- **Responder permissions**: `agy` 1.5.12 does not provide a selective per-tool allowlist that blocks workspace edit tools while allowing `trello_helper.py` and `gh`, so the responder runs with `--dangerously-skip-permissions` and enforces read-only workspace rules via prompt instructions.
- **No credential leaks**: The agent is explicitly forbidden from printing, commenting, or revealing any credentials, API keys, secrets, tokens, or passwords under any circumstances.
- This applies to Trello credentials, database passwords (like `db_reader`), `.env` secrets, or other configurations. Only the *presence* of these configurations may be verified.
