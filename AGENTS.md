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
- The Trello account used by the API may also be a real human's account. Do not blindly suppress the acting account's username unless the deployment intentionally wants to ignore that person's manual comments too.
- Suppress sidecar-authored comments by signature, e.g. `- Love Maeve`, and configurable known automation users through `TRELLO_SUPPRESSED_TRIGGER_USERNAMES`.
- Never let agent-authored Trello comments @-mention the acting account or users marked `never_at_mention` in stakeholder context. `trello_helper.py` rewrites disallowed mentions to the configured plain-text replacement before posting.
- When payloads provide only `cardLink`, resolve the short ID from `https://trello.com/c/<shortId>` and fetch live card details before deciding who authored the triggering comment.

## Reply Targeting

- The triggering comment author is the primary person to address.
- Usernames mentioned inside comment text are recipients/context, not proof of authorship.
- Do not tag a username merely because the comment text mentions that username.
- Honor `mention_policy` from the deployment's stakeholder context. If a user is marked `never_at_mention`, address them by plain-text preferred name.
- If author identity cannot be resolved from payload or live Trello state, keep the reply untagged or address the team generally.

## Stakeholder Context

- Keep deployment-specific people, roles, usernames, and mention policies in local config, not committed source.
- Preferred local path: `~/.gemini/antigravity-cli/trello_stakeholders.json`, or set `TRELLO_STAKEHOLDER_CONTEXT_FILE`.
- Use `TRELLO_STAKEHOLDER_CONTEXT_JSON` only for small runtime-managed configs.
- Stakeholder context should identify role, authority, preferred address, mention policy, and tone needs so the agent can adapt without hardcoded team names.

## Audience And Tone

- Trello is for PMs, designers, QA, and reporters. Keep Trello comments short, plain-language, and product-facing.
- Default Trello responses should be under 120 words or 3-6 bullets.
- Do not include file paths, class names, API routes, database fields, command/tool narration, or architecture details in Trello comments unless explicitly asked.
- For design-facing replies, talk about user flow, screen behavior, visual states, copy, layout, and decisions needed.
- Technical detail belongs in GitHub issues and PRs, not Trello card comments.
- Simple questions should get direct answers first. Do not restart planning, create issues, or move cards unless explicitly requested.
- During investigation, do not do deep code dives while product/design requirements are still unsettled. Do only light grounding to avoid impossible suggestions, then ask the needed questions. Save detailed code reading, implementation planning, and Codex review for planner mode after decisions are settled.

## Duplicate Work Prevention

- Before creating any GitHub issue, search existing issues and PRs in all configured repos.
- Search by Trello card URL, short card ID, card title keywords, and links already present on the card.
- Check `~/.gemini/antigravity-cli/brain/<conversation-id>/` for prior issue bodies, specs, and update scripts tied to the same card.
- If prior artifacts or GitHub results show matching work, update/link the existing issue or PR instead of creating a duplicate.
- If duplicate status is unclear, ask on Trello instead of creating new work.
- If related PRs are already merged or deployed and the reporter asks for additional changes, treat it as follow-up work. Prefer a new Trello card/GitHub issue linked back to the original card for context.
- If work is still active in an open issue/PR, update/link that active work instead of creating new duplicates.

## Codex MCP Review

- Use Codex as a bounded reviewer, not a co-author that rewrites the whole plan.
- Planner prompts to Codex should include: product goal, user-facing behavior, code paths inspected, proposed FE/BE split, acceptance criteria, open questions, and duplicate/progress findings.
- Ask Codex to return only: top risks, missing product decisions, likely duplicate/related work, test gaps, and concrete edits to the issue spec.
- Skip Codex for simple Trello replies, copy/layout clarifications, status updates, or questions that can be answered from existing card context.
- Skip Codex while requirements are still unsettled. Gather decisions first, then review the implementation plan.
- Never paste raw secrets or environment values into Codex prompts.

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

## Public Release Privacy

- Before publishing this repo outside the team, scan both the working tree and git history for private stakeholder names, usernames, project paths, URLs, and secrets.
- Removing data from current files is not enough for open source publication; plan a deliberate history rewrite and fresh verification before making the repository public.
