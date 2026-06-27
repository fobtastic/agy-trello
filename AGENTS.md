# AGENTS.md

Guidance for future coding agents working on this Antigravity Trello sidecar.

## Core Context

- This repo is a Trello webhook sidecar for `agy`.
- The active server entrypoint is `.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/server.py`.
- Plugin validation is done with `agy plugin validate .agents/plugins/trello-integration`.
- Trello card-to-conversation mappings live at `~/.gemini/antigravity-cli/trello_sessions.json`.
- Antigravity conversation artifacts live under `~/.gemini/antigravity-cli/brain/<conversation-id>/`.

## Safety Rules

- Never print or commit API keys, tokens, `.env` contents, OAuth tokens, or Trello/GitHub credentials.
- Do not add raw secret values to logs, README examples, tests, or fixtures.
- Treat `.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/.env` as local runtime config, not source documentation.
- The sidecar may run with `--dangerously-skip-permissions`; keep prompts and deterministic guards conservative.

## Trello Trigger Rules

- Prefer deterministic suppression before launching `agy`.
- Do not post acknowledgement comments unless explicitly enabled. Ack comments can trigger other board automations.
- The Trello account used by the API may also be Chris's real account. Do not blindly suppress `@fobtastic` by username.
- Suppress sidecar-authored comments by signature, e.g. `- Love Maeve`, and configurable known automation users through `TRELLO_SUPPRESSED_TRIGGER_USERNAMES`.
- Never let agent-authored Trello comments @-mention the acting account. By default `trello_helper.py` rewrites `@fobtastic` / `@**fobtastic**` to plain `Chris` before posting.
- When payloads provide only `cardLink`, resolve the short ID from `https://trello.com/c/<shortId>` and fetch live card details before deciding who authored the triggering comment.

## Reply Targeting

- The triggering comment author is the primary person to address.
- Usernames mentioned inside comment text are recipients/context, not proof of authorship.
- Do not tag `@fobtastic` merely because the comment says `@fobtastic`.
- Never tag `@fobtastic` in agent-authored comments. If Chris is the actual human commenter, address him as plain text `Chris`.
- If author identity cannot be resolved from payload or live Trello state, keep the reply untagged or address the team generally.

## Duplicate Work Prevention

- Before creating any GitHub issue, search existing issues and PRs in all configured repos.
- Search by Trello card URL, short card ID, card title keywords, and links already present on the card.
- Check `~/.gemini/antigravity-cli/brain/<conversation-id>/` for prior issue bodies, specs, and update scripts tied to the same card.
- If prior artifacts or GitHub results show matching work, update/link the existing issue or PR instead of creating a duplicate.
- If duplicate status is unclear, ask on Trello instead of creating new work.

## Verification Checklist

Run these before committing sidecar changes:

```bash
python3 -m py_compile .agents/plugins/trello-integration/sidecars/trello-webhook-receiver/server.py .agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py
agy plugin validate .agents/plugins/trello-integration
```

For runtime changes:

```bash
systemctl --user restart trello-webhook.service
systemctl --user status trello-webhook.service --no-pager
```

Then test against actual Trello card data. Prefer a non-mutating webhook POST to the local sidecar using an existing card link and a synthetic payload first. If posting a real Trello comment is necessary, use a clearly marked test card and confirm `TRELLO_POST_ACK_COMMENTS=false` first.
