import asyncio
import base64
import builtins
import copy
import errno
import hashlib
import hmac
import importlib.util
import io
import json
import logging
import os
import signal
import socket
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock


SERVER_PATH = (
    Path(__file__).resolve().parents[1]
    / ".agents/plugins/trello-integration/sidecars/trello-webhook-receiver/server.py"
)
HELPER_PATH = (
    Path(__file__).resolve().parents[1]
    / ".agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py"
)


def _isolated_exists(real_exists):
    def _check(path):
        if str(path).endswith((".zshrc", ".env", "trello_stakeholders.json")):
            return False
        return real_exists(path)

    return _check


def load_server(env_overrides=None):
    tempdir = Path(tempfile.mkdtemp())
    base_env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(tempdir),
        "TRELLO_AGENT_SIGNATURE_NAME": "Agy",
        "TRELLO_WEBHOOK_TOKEN": "test-webhook-token",
    }
    if env_overrides:
        base_env.update(env_overrides)
    real_exists = os.path.exists
    with (
        mock.patch.dict(os.environ, base_env, clear=True),
        mock.patch("os.path.exists", side_effect=_isolated_exists(real_exists)),
    ):
        spec = importlib.util.spec_from_file_location("trello_sidecar_server", SERVER_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    module.SESSION_MAP_FILE = str(tempdir / "trello_sessions.json")
    module.SIDECAR_STATE_FILE = str(tempdir / "sidecar_state.json")
    module.SIDECAR_EVENT_LOG_FILE = str(tempdir / "sidecar_events.jsonl")
    module.pending_cards.clear()
    module.shutdown_event.clear()
    module.outbound_posts_closed_event.clear()
    return module


def seed_queued_card(server, card_key, card_name="Card", payload=None, admission_id=None, action_id=None):
    norm_key = str(server.normalize_canonical_trello_id(card_key) or card_key)
    state = server.load_sidecar_state()
    entry = {
        "card_name": card_name,
        "payload": payload or {"cardId": norm_key},
        "queued_at": time.time(),
        "stage": "queued",
    }
    if admission_id is not None:
        entry["admission_id"] = admission_id
        if isinstance(payload, dict):
            payload["_admission_id"] = admission_id
    if action_id is not None:
        entry["action_id"] = action_id
    state.setdefault("in_flight_cards", {})[norm_key] = entry
    assert server.save_sidecar_state(state)


def load_helper(env_overrides=None):
    tempdir = Path(tempfile.mkdtemp())
    base_env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(tempdir),
        "TRELLO_API_KEY": "test-key",
        "TRELLO_API_TOKEN": "test-token",
        "TRELLO_AGENT_SIGNATURE_NAME": "Agy",
    }
    if env_overrides:
        base_env.update(env_overrides)
    real_exists = os.path.exists
    with (
        mock.patch.dict(os.environ, base_env, clear=True),
        mock.patch("os.path.exists", side_effect=_isolated_exists(real_exists)),
    ):
        spec = importlib.util.spec_from_file_location("trello_helper", HELPER_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


class FakeStreamProcess:
    def __init__(self, lines=None, stderr_bytes=b"", returncode=0, hang_after_lines=False, pid=None, force_returncode=None, raw_stdout_chunks=None):
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None
        self.pid = pid
        self.terminated = False
        self.killed = False
        self._hang = hang_after_lines
        self.returncode = force_returncode if force_returncode is not None else (None if hang_after_lines else returncode)
        for line in (lines or []):
            raw = line.encode("utf-8") if isinstance(line, str) else line
            if not raw.endswith(b"\n"):
                raw += b"\n"
            self.stdout.feed_data(raw)
        for chunk in (raw_stdout_chunks or []):
            raw = chunk.encode("utf-8") if isinstance(chunk, str) else chunk
            self.stdout.feed_data(raw)
        if not hang_after_lines:
            self.stdout.feed_eof()
        if stderr_bytes:
            self.stderr.feed_data(stderr_bytes)
        if not hang_after_lines:
            self.stderr.feed_eof()

    def _feed_eofs(self):
        self.stdout.feed_eof()
        self.stderr.feed_eof()

    def _close_streams(self):
        if self._loop is not None and not self._loop.is_closed():
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if running is not self._loop:
                self._loop.call_soon_threadsafe(self._feed_eofs)
                return
        self._feed_eofs()

    def terminate(self):
        self.terminated = True
        self.returncode = -15
        self._close_streams()

    def kill(self):
        self.killed = True
        self.returncode = -9
        self._close_streams()

    async def wait(self):
        return self.returncode


class SidecarPolicyTests(unittest.TestCase):
    def test_agent_ack_comments_are_suppressed(self):
        server = load_server()

        self.assertTrue(
            server.is_agent_or_suppressed_comment(
                "Got it, I am checking this now.\n\n[agy-sidecar:ack]",
                "some-agent-user",
            )
        )

    def test_repetitive_agent_comment_without_new_substance_is_suppressed(self):
        server = load_server()
        payload = {
            "action": {
                "type": "commentCard",
                "memberCreator": {"username": "product-agent", "fullName": "Product Agent"},
            },
            "recentComments": [
                {
                    "text": "I think we should ask Product to confirm the empty state copy before spec.",
                    "memberCreator": {"username": "owner-agent"},
                }
            ],
        }

        accepted, reason = server.should_accept_trigger(
            "card-1",
            "Card",
            payload,
            "Agreed, we should ask Product to confirm the empty state copy before spec. [agy-sidecar:comment]",
            "Ideas",
        )

        self.assertFalse(accepted)
        self.assertEqual(reason, "low_novelty_agent_comment")

    def test_agent_comment_with_new_question_is_allowed(self):
        server = load_server()
        payload = {
            "action": {
                "type": "commentCard",
                "memberCreator": {"username": "product-agent", "fullName": "Product Agent"},
            },
            "recentComments": [
                {
                    "text": "The current card says to improve the apply funnel.",
                    "memberCreator": {"username": "owner-agent"},
                }
            ],
        }

        accepted, reason = server.should_accept_trigger(
            "card-2",
            "Card",
            payload,
            "Question: should the new apply funnel modal remember the user's last selected resume? [agy-sidecar:comment]",
            "Ideas",
        )

        self.assertTrue(accepted)
        self.assertEqual(reason, "accepted")

    def test_ack_policy_only_allows_long_running_modes_once_per_cooldown(self):
        server = load_server()
        now = time.time()
        state = {
            "last_ack_by_card": {
                "card-1": now - 30,
            }
        }

        self.assertFalse(server.should_post_ack_comment("card-1", "GENERAL", state, now=now))
        self.assertFalse(server.should_post_ack_comment("card-1", "INVESTIGATOR", state, now=now))
        self.assertTrue(server.should_post_ack_comment("card-2", "READY_FOR_SPEC", state, now=now))

    def test_ack_text_contains_suppressible_marker_and_no_mentions(self):
        server = load_server()

        ack = server.build_ack_comment()

        self.assertIn("<!-- [agy-sidecar:ack] -->", ack)
        self.assertNotIn("@", ack)

    def test_failure_comment_contains_suppressible_marker_and_no_mentions(self):
        server = load_server()

        failure = server.build_failure_comment()

        self.assertIn("<!-- [agy-sidecar:status] -->", failure)
        self.assertNotIn("@", failure)
        # The failure comment must not re-trigger the webhook loop.
        self.assertTrue(server.is_agent_or_suppressed_comment(failure, "some-agent-user"))

    def test_helper_appends_substantive_agent_marker_once(self):
        helper = load_helper()

        marked = helper.ensure_agent_comment_marker("Question: should we keep this modal?")

        self.assertIn("<!-- [agy-sidecar:comment] -->", marked)
        self.assertEqual(marked.count("<!-- [agy-sidecar:comment] -->"), 1)
        self.assertEqual(
            helper.ensure_agent_comment_marker(marked).count("<!-- [agy-sidecar:comment] -->"),
            1,
        )

        # Ensure legacy markers are recognized and NOT duplicated or overwritten
        legacy_marked = "Question: should we keep this modal?\n\n[agy-sidecar:comment]"
        self.assertEqual(helper.ensure_agent_comment_marker(legacy_marked), legacy_marked)

    def test_structured_event_log_appends_redacted_jsonl(self):
        server = load_server()

        server.log_sidecar_event(
            "trigger_ignored",
            {
                "card_id": "card-123",
                "card_name": "Card",
                "reason": "suppressed_trigger_member",
                "url": "https://api.trello.com/1/cards/card-123?key=secret-key&token=secret-token",
                "comment": "This full comment body should not be logged",
            },
        )

        lines = Path(server.SIDECAR_EVENT_LOG_FILE).read_text().strip().splitlines()
        self.assertEqual(len(lines), 1)
        event = json.loads(lines[0])
        self.assertEqual(event["event"], "trigger_ignored")
        self.assertEqual(event["card_id"], "card-123")
        self.assertEqual(event["reason"], "suppressed_trigger_member")
        self.assertNotIn("comment", event)
        self.assertEqual(event["url"], "https://api.trello.com/1/cards/card-123?key=[REDACTED]&token=[REDACTED]")
        self.assertIn("ts", event)

    def test_trigger_decision_logs_accept_and_ignore_events(self):
        server = load_server()

        ignored, reason = server.should_accept_trigger(
            "card-ignored",
            "Ignored Card",
            {
                "action": {
                    "id": "action-ignored",
                    "type": "commentCard",
                    "memberCreator": {"username": "trello", "fullName": "Trello Automation"},
                }
            },
            "Automation ping",
            "Ideas",
        )
        accepted, accepted_reason = server.should_accept_trigger(
            "card-accepted",
            "Accepted Card",
            {
                "action": {
                    "id": "action-accepted",
                    "type": "commentCard",
                    "memberCreator": {"username": "human-user", "fullName": "Human User"},
                }
            },
            "Can we make this empty state clearer?",
            "Ideas",
        )

        self.assertFalse(ignored)
        self.assertEqual(reason, "suppressed_trigger_member")
        self.assertTrue(accepted)
        self.assertEqual(accepted_reason, "accepted")
        events = [
            json.loads(line)
            for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().strip().splitlines()
        ]
        self.assertEqual([event["event"] for event in events], ["trigger_ignored", "trigger_accepted"])
        self.assertEqual(events[0]["reason"], "suppressed_trigger_member")
        self.assertEqual(events[1]["card_id"], "card-accepted")


    def test_in_flight_cards_persist_requeue_and_clear(self):
        server = load_server()
        payload = {
            "cardName": "Restart Card",
            "action": {
                "id": "action-restart",
                "type": "commentCard",
                "memberCreator": {"username": "human-user", "fullName": "Human User"},
            },
        }
        dispatched_payload = {
            "cardName": "Interrupted Card",
            "cardLink": "https://trello.com/c/IntCard1",
            "action": {
                "id": "action-dispatched",
                "type": "commentCard",
                "memberCreator": {"username": "human-user", "fullName": "Human User"},
            },
        }

        accepted, _ = server.should_accept_trigger(
            "card-restart", "Restart Card", payload, "Please look at this", "Ideas"
        )
        accepted_disp, _ = server.should_accept_trigger(
            "card-dispatched", "Interrupted Card", dispatched_payload, "Did this run?", "Ideas"
        )
        self.assertTrue(accepted)
        self.assertTrue(accepted_disp)
        self.assertTrue(
            server.mark_in_flight_dispatched(
                "card-dispatched",
                conversation_id="conv-disp-1",
                admission_id=dispatched_payload.get("_admission_id"),
            )
        )

        state = json.loads(Path(server.SIDECAR_STATE_FILE).read_text())
        self.assertEqual(state["in_flight_cards"]["card-restart"]["stage"], "queued")
        self.assertEqual(state["in_flight_cards"]["card-dispatched"]["stage"], "dispatched")

        # Simulate a process restart: queued cards requeue, already-dispatched cards quarantine without replay.
        server.pending_cards.clear()
        self.assertTrue(server.requeue_in_flight_cards())
        card_key, card_name_str, requeued_payload = server.task_queue.get_nowait()
        self.assertTrue(server.task_queue.empty())
        self.assertEqual(card_key, "card-restart")
        self.assertEqual(card_name_str, "Restart Card")
        self.assertEqual(requeued_payload["cardName"], "Restart Card")
        self.assertIn("card-restart", server.pending_cards)
        self.assertNotIn("card-dispatched", server.pending_cards)

        state = json.loads(Path(server.SIDECAR_STATE_FILE).read_text())
        self.assertNotIn("card-dispatched", state.get("in_flight_cards", {}))
        expected_unrec_key = f"card-dispatched:{dispatched_payload['_admission_id']}"
        self.assertIn(expected_unrec_key, state.get("unreconciled_cards", {}))
        self.assertNotIn("card-dispatched:action-dispatched", state.get("unreconciled_cards", {}))
        self.assertEqual(state["unreconciled_cards"][expected_unrec_key].get("action_id"), "action-dispatched")
        events = [json.loads(line) for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertTrue(any(e["event"] == "trigger_interrupted_unreconciled" and e["card_id"] == "card-dispatched" for e in events))

        # Completion failure logs an event, returns False, and still never allows replay if stage was dispatched.
        self.assertTrue(
            server.mark_in_flight_dispatched(
                "card-restart",
                conversation_id="conv-disp-2",
                admission_id=requeued_payload.get("_admission_id"),
            )
        )
        with mock.patch.object(server, "save_sidecar_state", return_value=False):
            self.assertFalse(server.release_pending_card("card-restart", admission_id=requeued_payload.get("_admission_id")))
        events = [json.loads(line) for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertEqual(events[-1]["event"], "in_flight_release_failed")
        server.pending_cards.clear()
        server.requeue_in_flight_cards()
        self.assertTrue(server.task_queue.empty())

        # Successful completion clears the persisted in-flight entry cleanly.
        accepted_clean, _ = server.should_accept_trigger(
            "card-clean", "Clean Card", {"cardName": "Clean Card"}, "Fresh request", "Ideas"
        )
        self.assertTrue(accepted_clean)
        self.assertTrue(server.release_pending_card("card-clean"))
        state = json.loads(Path(server.SIDECAR_STATE_FILE).read_text())
        self.assertNotIn("card-clean", state.get("in_flight_cards", {}))
        self.assertNotIn("card-clean", server.pending_cards)

        # Failed dispatch transition fails closed before launching any Agy subprocess.
        accepted_fail, _ = server.should_accept_trigger(
            "card-fail-dispatch", "Fail Dispatch", {"cardName": "Fail Dispatch"}, "Run", "Ideas"
        )
        self.assertTrue(accepted_fail)
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server, "mark_in_flight_dispatched", return_value=False),
            mock.patch.object(server.asyncio, "create_subprocess_exec") as mock_exec,
        ):
            with self.assertRaises(RuntimeError):
                asyncio.run(
                    server.trigger_agent(
                        "card-fail-dispatch",
                        "Fail Dispatch",
                        {"cardId": "card-fail-dispatch", "comment": "Run"},
                    )
                )
        mock_exec.assert_not_called()

        # Real unreadable/corrupt state file during admitted trigger_agent also fails closed (0 subprocesses).
        accepted_corrupt, _ = server.should_accept_trigger(
            "69aaee111111111111111111",
            "Corrupt State",
            {"cardName": "Corrupt State", "action": {"id": "act-corrupt-1", "type": "commentCard"}},
            "Run",
            "Ideas",
        )
        self.assertTrue(accepted_corrupt)
        Path(server.SIDECAR_STATE_FILE).write_text("{corrupt-json")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server.asyncio, "create_subprocess_exec") as mock_exec,
        ):
            with self.assertRaises(server.DeferredTriggerRetry):
                asyncio.run(
                    server.trigger_agent(
                        "69aaee111111111111111111",
                        "Corrupt State",
                        {"cardId": "69aaee111111111111111111", "comment": "Run"},
                    )
                )
        mock_exec.assert_not_called()

        # Missing persisted admission row fails closed (0 subprocesses).
        Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({"recent_triggers": [], "card_activity": {}, "in_flight_cards": {}}))
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server.asyncio, "create_subprocess_exec") as mock_exec,
        ):
            with self.assertRaises(RuntimeError):
                asyncio.run(
                    server.trigger_agent(
                        "69aaee222222222222222222",
                        "Missing Row",
                        {"cardId": "69aaee222222222222222222", "comment": "Run"},
                    )
                )
        mock_exec.assert_not_called()

        # Quarantined unreconciled action stays suppressed even after recent_triggers and card_activity expire.
        quarantined_state = {
            "recent_triggers": [],
            "card_activity": {},
            "in_flight_cards": {},
            "unreconciled_cards": {
                "69aaee333333333333333333": {
                    "card_name": "Interrupted Card",
                    "action_id": "act-dispatched-q1",
                    "stage": "dispatched",
                }
            },
        }
        Path(server.SIDECAR_STATE_FILE).write_text(json.dumps(quarantined_state))
        server.pending_cards.clear()
        re_acc, re_reason = server.should_accept_trigger(
            "69aaee333333333333333333",
            "Interrupted Card",
            {"cardId": "69aaee333333333333333333", "action": {"id": "act-dispatched-q1", "type": "commentCard"}},
            "Replay after TTL",
            "Ideas",
        )
        self.assertFalse(re_acc)
        self.assertEqual(re_reason, "unreconciled_action")
        new_acc, new_reason = server.should_accept_trigger(
            "69aaee333333333333333333",
            "Interrupted Card",
            {"cardId": "69aaee333333333333333333", "action": {"id": "act-fresh-q2", "type": "commentCard"}},
            "Distinct new comment",
            "Ideas",
        )
        self.assertTrue(new_acc)
        self.assertEqual(new_reason, "accepted")

        # Two sequential interrupted actions on the same card both retain replay guards across restarts.
        Path(server.SIDECAR_STATE_FILE).write_text("{}")
        server.pending_cards.clear()
        p_a = {"cardId": "69aaee444444444444444444", "action": {"id": "act-seq-a", "type": "commentCard"}}
        self.assertTrue(server.should_accept_trigger("69aaee444444444444444444", "Multi Card", p_a, "Run A", "Ideas")[0])
        self.assertTrue(server.mark_in_flight_dispatched("69aaee444444444444444444", admission_id=p_a["_admission_id"]))
        server.pending_cards.clear()
        self.assertTrue(server.requeue_in_flight_cards())

        p_b = {"cardId": "69aaee444444444444444444", "action": {"id": "act-seq-b", "type": "commentCard"}}
        self.assertTrue(server.should_accept_trigger("69aaee444444444444444444", "Multi Card", p_b, "Run B", "Ideas")[0])
        self.assertTrue(server.mark_in_flight_dispatched("69aaee444444444444444444", admission_id=p_b["_admission_id"]))
        server.pending_cards.clear()
        self.assertTrue(server.requeue_in_flight_cards())

        aged = server.load_sidecar_state()
        aged["recent_triggers"] = []
        aged["card_activity"] = {}
        self.assertTrue(server.save_sidecar_state(aged))
        self.assertEqual(
            server.should_accept_trigger("69aaee444444444444444444", "Multi Card", {"cardId": "69aaee444444444444444444", "action": {"id": "act-seq-a", "type": "commentCard"}}, "Replay A", "Ideas"),
            (False, "unreconciled_action"),
        )
        self.assertEqual(
            server.should_accept_trigger("69aaee444444444444444444", "Multi Card", {"cardId": "69aaee444444444444444444", "action": {"id": "act-seq-b", "type": "commentCard"}}, "Replay B", "Ideas"),
            (False, "unreconciled_action"),
        )
        self.assertEqual(
            server.should_accept_trigger("69aaee444444444444444444", "Multi Card", {"cardId": "69aaee444444444444444444", "action": {"id": "act-seq-c", "type": "commentCard"}}, "Fresh C", "Ideas"),
            (True, "accepted"),
        )

        # Failed quarantine save on startup leaves dispatched row on disk and blocks new admission on that card.
        Path(server.SIDECAR_STATE_FILE).write_text("{}")
        server.pending_cards.clear()
        p_fail_q = {"cardId": "69aaee555555555555555555", "action": {"id": "act-fail-q", "type": "commentCard"}}
        self.assertTrue(server.should_accept_trigger("69aaee555555555555555555", "Persist Fail Card", p_fail_q, "Run", "Ideas")[0])
        self.assertTrue(server.mark_in_flight_dispatched("69aaee555555555555555555", admission_id=p_fail_q["_admission_id"]))
        server.pending_cards.clear()
        with mock.patch.object(server, "save_sidecar_state", return_value=False):
            self.assertFalse(server.requeue_in_flight_cards())
        residual = server.load_sidecar_state()
        self.assertEqual(residual["in_flight_cards"]["69aaee555555555555555555"]["stage"], "dispatched")
        self.assertEqual(
            server.should_accept_trigger("69aaee555555555555555555", "Persist Fail Card", {"cardId": "69aaee555555555555555555", "action": {"id": "act-new-after-fail", "type": "commentCard"}}, "New run", "Ideas"),
            (False, "state_persistence_failed"),
        )

        # Residual legacy queued row whose startup migration save failed returns 503 via HTTP (never 200 card_already_running),
        # recovers cleanly once storage is healthy, and run() aborts startup if reconciliation never succeeds.
        Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({
            "in_flight_cards": {
                "69AAEE666666666666666666": {
                    "card_name": "Residual Legacy Queued",
                    "payload": {"cardId": "69AAEE666666666666666666", "comment": "Orig ask"},
                    "queued_at": time.time(),
                    "stage": "queued",
                }
            }
        }))
        server.pending_cards.clear()
        while not server.task_queue.empty():
            server.task_queue.get_nowait()
        with mock.patch.object(server, "save_sidecar_state", return_value=False):
            self.assertFalse(server.requeue_in_flight_cards())
        server.AUTH_TOKEN = "secret-webhook-token"
        httpd = server.BoundedThreadingHTTPServer(("127.0.0.1", 0), server.WebhookHandler, max_workers=2)
        port = httpd.server_address[1]
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        try:
            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                req_res = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps({
                        "cardId": "69aaee666666666666666666",
                        "comment": "Orig ask",
                        "triggerUserUsername": "pm_user",
                    }).encode("utf-8"),
                    headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx_503_legacy:
                    urllib.request.urlopen(req_res, timeout=5)
                self.assertEqual(ctx_503_legacy.exception.code, 503)
                ctx_503_legacy.exception.close()

            # Once storage recovers, original action reconciles to a single queue item.
            self.assertTrue(server.requeue_in_flight_cards())
            q_leg_key, _, _ = server.task_queue.get_nowait()
            self.assertEqual(q_leg_key, "69aaee666666666666666666")
            self.assertTrue(server.task_queue.empty())

            # ID-less relay event quarantined on interruption blocks redelivery even after distinct card activity,
            # while distinct events still enter cleanly.
            Path(server.SIDECAR_STATE_FILE).write_text("{}")
            server.pending_cards.clear()
            while not server.task_queue.empty():
                server.task_queue.get_nowait()
            idless_body = {
                "cardId": "69aaee777777777777777777",
                "comment": "ID-less deploy request",
                "triggerUserUsername": "pm_user",
            }
            req_idless = urllib.request.Request(
                f"http://127.0.0.1:{port}/",
                method="POST",
                data=json.dumps(idless_body).encode("utf-8"),
                headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
            )
            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                with urllib.request.urlopen(req_idless, timeout=5) as resp_idless:
                    self.assertEqual(resp_idless.status, 202)
                k_idl, _, p_idl = server.task_queue.get_nowait()
                self.assertTrue(server.mark_in_flight_dispatched(k_idl, admission_id=p_idl["_admission_id"], action_id="act-live-learned"))
                server.pending_cards.clear()
                self.assertTrue(server.requeue_in_flight_cards())

                # Verify live-learned action_id and ID-less fingerprint coexist on one unified tombstone record.
                q_state = server.load_sidecar_state()
                tombstones = q_state.get("unreconciled_tombstones", {})
                self.assertEqual(len(tombstones), 1)
                only_tomb = next(iter(tombstones.values()))
                self.assertEqual(only_tomb.get("action_id"), "act-live-learned")
                self.assertTrue(only_tomb.get("fingerprints"))
                self.assertNotIn("unreconciled_action_ids", q_state)
                self.assertNotIn("unreconciled_fingerprints", q_state)

                # Run a distinct event to replace card_activity fingerprint first.
                req_other = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps({"cardId": "69aaee777777777777777777", "comment": "Distinct follow-up", "triggerUserUsername": "pm_user"}).encode("utf-8"),
                    headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req_other, timeout=5) as resp_other:
                    self.assertEqual(resp_other.status, 202)
                k_oth, _, p_oth = server.task_queue.get_nowait()
                self.assertTrue(server.release_pending_card(k_oth, admission_id=p_oth["_admission_id"]))

                # Now replay the original ID-less event: still blocked by durable fingerprint tombstone.
                req_replay_idless = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps(idless_body).encode("utf-8"),
                    headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req_replay_idless, timeout=5) as resp_rep:
                    self.assertEqual(json.loads(resp_rep.read().decode("utf-8")), {"status": "ignored", "reason": "unreconciled_action"})
        finally:
            httpd.shutdown()
            httpd.server_close()

        # Startup run() refuses to serve HTTP if all reconciliation retries fail.
        with (
            mock.patch.object(server, "requeue_in_flight_cards", return_value=False) as mock_requeue,
            mock.patch.object(server.time, "sleep"),
            mock.patch.object(server, "BoundedThreadingHTTPServer") as mock_httpd_cls,
        ):
            with self.assertRaises(RuntimeError):
                server.run(port=0, reconcile_retry_delay=0.0)
        self.assertEqual(mock_requeue.call_count, 3)
        mock_httpd_cls.assert_not_called()

        # Capping unreconciled_cards at 500 still preserves tombstones for pruned entries until reconciled.
        large_unreconciled = {
            f"69bb00{i:018d}:act-old-{i}": {
                "card_id": f"69bb00{i:018d}",
                "action_id": f"act-old-{i}",
                "stage": "dispatched",
            }
            for i in range(505)
        }
        large_action_tombstones = {f"act-old-{i}": f"69bb00{i:018d}" for i in range(505)}
        Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({
            "unreconciled_cards": large_unreconciled,
            "unreconciled_action_ids": large_action_tombstones,
            "in_flight_cards": {
                "69aaee888888888888888888": {
                    "card_name": "Cap Card",
                    "payload": {"cardId": "69aaee888888888888888888", "action": {"id": "act-cap-new"}},
                    "stage": "dispatched",
                    "action_id": "act-cap-new",
                }
            },
        }))
        server.pending_cards.clear()
        self.assertTrue(server.requeue_in_flight_cards())
        capped_state = server.load_sidecar_state()
        self.assertEqual(len(capped_state["unreconciled_cards"]), 500)
        self.assertNotIn("69bb00000000000000000000:act-old-0", capped_state["unreconciled_cards"])
        self.assertEqual(
            server.should_accept_trigger("69bb00000000000000000000", "Evicted", {"cardId": "69bb00000000000000000000", "action": {"id": "act-old-0"}}, "Replay oldest", "Ideas"),
            (False, "unreconciled_action"),
        )
        self.assertTrue(server.reconcile_quarantined_entry(action_id="act-old-0"))
        self.assertEqual(
            server.should_accept_trigger("69bb00000000000000000000", "Evicted", {"cardId": "69bb00000000000000000000", "action": {"id": "act-old-0"}}, "Explicit retry", "Ideas"),
            (True, "accepted"),
        )

        # Legacy state containing 505 unreconciled_cards detail rows with NO tombstone index still preserves all 505 on upgrade & prune.
        legacy_detail_only = {
            f"69bc00{i:018d}:act-leg-{i}": {
                "card_id": f"69bc00{i:018d}",
                "action_id": f"act-leg-{i}",
                "stage": "dispatched",
                "fingerprint": f"fp-leg-{i}",
            }
            for i in range(505)
        }
        Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({
            "unreconciled_cards": legacy_detail_only,
            "in_flight_cards": {
                "69bcff888888888888888888": {
                    "card_name": "Upgrade Cap Card",
                    "payload": {"cardId": "69bcff888888888888888888", "action": {"id": "act-upg-new"}},
                    "stage": "dispatched",
                    "action_id": "act-upg-new",
                }
            },
        }))
        server.pending_cards.clear()
        self.assertTrue(server.requeue_in_flight_cards())
        upg_state = server.load_sidecar_state()
        self.assertEqual(len(upg_state["unreconciled_cards"]), 500)
        self.assertNotIn("69bc00000000000000000000:act-leg-0", upg_state["unreconciled_cards"])
        self.assertGreaterEqual(len(upg_state.get("unreconciled_tombstones", {})), 506)
        self.assertEqual(
            server.should_accept_trigger("69bc00000000000000000000", "Legacy Evicted", {"cardId": "69bc00000000000000000000", "action": {"id": "act-leg-0"}}, "Replay legacy", "Ideas"),
            (False, "unreconciled_action"),
        )

    def test_default_model_and_build_agy_command(self):
        server = load_server()
        self.assertEqual(server.DEFAULT_AGENT_MODEL, "gemini-pro-orchestrator")
        self.assertEqual(server.CLASSIFIER_MODEL, "gemini-pro-orchestrator")

        overridden = load_server({"TRELLO_AGENT_MODEL": "gemini-4-argon-medium"})
        self.assertEqual(overridden.DEFAULT_AGENT_MODEL, "gemini-4-argon-medium")
        self.assertEqual(overridden.CLASSIFIER_MODEL, "gemini-4-argon-medium")

        with tempfile.TemporaryDirectory() as tmpdir:
            ws1 = Path(tmpdir) / "be"
            ws2 = Path(tmpdir) / "fe"
            missing = Path(tmpdir) / "missing"
            ws1.mkdir()
            ws2.mkdir()
            cmd = server.build_agy_command(
                server.DEFAULT_AGENT_MODEL,
                "Inspect card",
                workspaces=[str(ws1), str(ws2), str(missing)],
                target_cwd=str(ws1),
                conversation_id="conv-123",
                print_timeout="15m",
            )

        self.assertIn("--model", cmd)
        self.assertEqual(cmd[cmd.index("--model") + 1], "gemini-pro-orchestrator")
        self.assertEqual(cmd[cmd.index("--output-format") + 1], "stream-json")
        self.assertEqual(cmd[cmd.index("--print-timeout") + 1], "15m")
        self.assertEqual(cmd[cmd.index("--conversation") + 1], "conv-123")
        self.assertEqual(cmd[-2:], ["--print", "Inspect card"])
        add_dirs = [cmd[i + 1] for i, tok in enumerate(cmd) if tok == "--add-dir"]
        self.assertEqual(add_dirs, [str(ws1), str(ws2)])

        helper_rules = server.build_trello_helper_rules("/tmp/path with spaces/trello_helper.py")
        self.assertIn("'/tmp/path with spaces/trello_helper.py'", helper_rules)

    def test_parse_and_evaluate_agy_json_output_strict_validation(self):
        server = load_server()

        parsed_clean = server.parse_agy_json_output(
            '{"conversation_id":"abc-1","status":"SUCCESS","response":"Done","usage":{"total_tokens":120}}'
        )
        self.assertEqual(parsed_clean["conversation_id"], "abc-1")
        self.assertEqual(
            server.evaluate_agy_result(0, parsed_clean, "", require_conversation_id=True),
            (True, None),
        )

        stream_stdout = (
            '{"event":"init","conversation_id":"abc-2"}\n'
            '{"event":"result","result":{"conversation_id":"abc-2","status":"SUCCESS","response":"OK","usage":{"total_tokens":42}}}\n'
        )
        parsed_stream = server.parse_agy_json_output(stream_stdout)
        self.assertEqual(parsed_stream["conversation_id"], "abc-2")
        self.assertEqual(
            server.evaluate_agy_result(0, parsed_stream, "", require_conversation_id=True),
            (True, None),
        )

        # Init-only output without result envelope must fail on exit 0.
        self.assertIsNone(server.parse_agy_json_output('{"event":"init","conversation_id":"abc-only"}'))
        self.assertFalse(server.evaluate_agy_result(0, None, "")[0])
        self.assertFalse(server.evaluate_agy_result(0, {"response": "no status"}, "")[0])
        self.assertFalse(server.evaluate_agy_result(0, {"status": "SUCCESS"}, "")[0])
        self.assertFalse(server.evaluate_agy_result(0, {"status": "SUCCESS", "usage": "bad"}, "")[0])
        self.assertFalse(server.evaluate_agy_result(0, {"status": "SUCCESS", "usage": {"total_tokens": True}}, "")[0])
        self.assertFalse(server.evaluate_agy_result(0, {"status": "SUCCESS", "usage": {"total_tokens": 12.5}}, "")[0])
        self.assertFalse(server.evaluate_agy_result(0, {"status": "SUCCESS", "usage": {"total_tokens": "42"}}, "")[0])
        self.assertFalse(server.evaluate_agy_result(0, {"status": "SUCCESS", "usage": {"total_tokens": -1}}, "")[0])
        self.assertFalse(
            server.evaluate_agy_result(0, {"status": "SUCCESS", "conversation_id": "", "usage": {"total_tokens": 10}}, "", require_conversation_id=True)[0]
        )
        for bad_cid in [12345, {"a": 1}, True, "--flag", "../bad", "ab"]:
            self.assertFalse(
                server.evaluate_agy_result(
                    0,
                    {"status": "SUCCESS", "conversation_id": bad_cid, "usage": {"total_tokens": 10}},
                    "",
                    require_conversation_id=True,
                    init_conversation_id="valid-init-1234",
                )[0]
            )

        ok_rc, err_rc = server.evaluate_agy_result(1, {"status": "ERROR", "error": "invalid model"}, "")
        self.assertFalse(ok_rc)
        self.assertIn("invalid model", err_rc)

        ok_status, err_status = server.evaluate_agy_result(0, {"status": "ERROR", "error": "boom"}, "")
        self.assertFalse(ok_status)
        self.assertIn("boom", err_status)

        ok_err, err_field = server.evaluate_agy_result(0, {"status": "SUCCESS", "error": "bad auth"}, "")
        self.assertFalse(ok_err)
        self.assertIn("bad auth", err_field)

        # Safety block fails even with leading whitespace and non-zero tokens.
        ok_filter, err_filter = server.evaluate_agy_result(
            0,
            {
                "status": "SUCCESS",
                "response": "  \nThis request was blocked by Gemini's filters. Please try rephrasing.",
                "usage": {"total_tokens": 12},
            },
            "",
        )
        self.assertFalse(ok_filter)
        self.assertIn("safety filters", err_filter)

    def test_same_title_cards_keep_distinct_sessions(self):
        server = load_server()
        with tempfile.TemporaryDirectory() as ws_dir:
            server.WORKSPACES = [ws_dir]

            def make_card(card_id, title):
                return {
                    "id": card_id,
                    "shortLink": card_id,
                    "name": title,
                    "desc": "Desc",
                    "list": {"name": "Ideas"},
                    "actions": [
                        {
                            "id": f"act-{card_id}",
                            "type": "commentCard",
                            "date": "2026-11-19T00:00:00Z",
                            "data": {"text": "Check this"},
                            "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                        }
                    ],
                }

            seen_cmds = []
            counter = {"n": 0}

            async def fake_exec(*cmd, **kwargs):
                seen_cmds.append(list(cmd))
                if "--conversation" in cmd:
                    cid = cmd[cmd.index("--conversation") + 1]
                else:
                    counter["n"] += 1
                    cid = f"conv-{counter['n']}"
                lines = [
                    json.dumps({"event": "init", "conversation_id": cid}),
                    json.dumps({
                        "event": "result",
                        "result": {
                            "conversation_id": cid,
                            "status": "SUCCESS",
                            "response": "Done",
                            "usage": {"total_tokens": 50},
                        },
                    }),
                ]
                return FakeStreamProcess(lines=lines, returncode=0)

            with (
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_exec),
            ):
                with mock.patch.object(server, "fetch_card_details_sync", return_value=make_card("cardA1", "Shared Title")):
                    seed_queued_card(server, "cardA1", "Shared Title")
                    asyncio.run(
                        server.trigger_agent(
                            "cardA1",
                            "Shared Title",
                            {"cardId": "cardA1", "cardName": "Shared Title", "cardLink": "https://trello.com/c/cardA1", "comment": "Check this"},
                        )
                    )
                with mock.patch.object(server, "fetch_card_details_sync", return_value=make_card("cardB2", "Shared Title")):
                    seed_queued_card(server, "cardB2", "Shared Title")
                    asyncio.run(
                        server.trigger_agent(
                            "cardB2",
                            "Shared Title",
                            {"cardId": "cardB2", "cardName": "Shared Title", "cardLink": "https://trello.com/c/cardB2", "comment": "Check this"},
                        )
                    )
                    # Second run on cardB2 must resume conv-2, not conv-1.
                    seed_queued_card(server, "cardB2", "Shared Title")
                    asyncio.run(
                        server.trigger_agent(
                            "cardB2",
                            "Shared Title",
                            {"cardId": "cardB2", "cardName": "Shared Title", "cardLink": "https://trello.com/c/cardB2", "comment": "Check this"},
                        )
                    )

            self.assertNotIn("--conversation", seen_cmds[1])
            self.assertEqual(seen_cmds[2][seen_cmds[2].index("--conversation") + 1], "conv-2")
            session_map = json.loads(Path(server.SESSION_MAP_FILE).read_text())
            self.assertEqual(session_map["cardA1"], "conv-1")
            self.assertEqual(session_map["cardB2"], "conv-2")
            self.assertNotIn("Shared Title", session_map)

            # Mismatched cardLink pointing to cardB2 must neither attach cardB2's artifacts/session nor leak into prompt/preflight.
            with tempfile.TemporaryDirectory() as fake_home:
                brain_b = Path(fake_home) / ".gemini/antigravity-cli/brain/conv-2"
                brain_b.mkdir(parents=True)
                (brain_b / "spec.md").write_text("CARD_B_SECRET_SPEC")
                Path(server.SESSION_MAP_FILE).write_text(json.dumps({"cardB2": "conv-2"}))
                seen_cmds.clear()
                counter["n"] = 10

                # Subcase A: card A unmapped, live fetch succeeds for card A while relay supplies stale cardB2 link.
                with (
                    mock.patch.dict(os.environ, {"HOME": fake_home}, clear=False),
                    mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                    mock.patch.object(server, "fetch_card_details_sync", return_value=make_card("69aaaa111111111111111111", "Card A")),
                    mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_exec),
                ):
                    seed_queued_card(server, "69aaaa111111111111111111", "Card A")
                    asyncio.run(
                        server.trigger_agent(
                            "69aaaa111111111111111111",
                            "Card A",
                            {
                                "cardId": "69aaaa111111111111111111",
                                "cardShortId": "cardB2",
                                "cardLink": "https://trello.com/c/cardB2",
                                "comment": "Check A",
                            },
                        )
                    )
                self.assertNotIn("--conversation", seen_cmds[-1])
                self.assertNotIn("CARD_B_SECRET_SPEC", seen_cmds[-1][-1])
                session_map = json.loads(Path(server.SESSION_MAP_FILE).read_text())
                self.assertEqual(session_map["cardB2"], "conv-2")
                self.assertNotEqual(session_map["69aaaa111111111111111111"], "conv-2")

                # Subcase B: cold cache + outage + card titled with its own canonical ID: unverified link is stripped from prompt/preflight.
                a_conv = session_map["69aaaa111111111111111111"]
                state_cold = server.load_sidecar_state()
                state_cold.pop("card_aliases", None)
                state_cold.pop("canonical_cards", None)
                server.save_sidecar_state(state_cold)
                with (
                    mock.patch.dict(os.environ, {"HOME": fake_home}, clear=False),
                    mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                    mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                    mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_exec),
                ):
                    seed_queued_card(server, "69aaaa111111111111111111", "69aaaa111111111111111111")
                    asyncio.run(
                        server.trigger_agent(
                            "69aaaa111111111111111111",
                            "69aaaa111111111111111111",
                            {
                                "cardId": "69aaaa111111111111111111",
                                "cardName": "69aaaa111111111111111111",
                                "cardShortId": "cardB2",
                                "cardLink": "https://trello.com/c/cardB2",
                                "comment": "Second check A",
                            },
                        )
                    )
                self.assertEqual(seen_cmds[-1][seen_cmds[-1].index("--conversation") + 1], a_conv)
                self.assertNotIn("CARD_B_SECRET_SPEC", seen_cmds[-1][-1])
                self.assertNotIn("cardB2", seen_cmds[-1][-1])
                session_map = json.loads(Path(server.SESSION_MAP_FILE).read_text())
                self.assertEqual(session_map["cardB2"], "conv-2")

                # Subcase C: READY_FOR_SPEC with no verified link defers before preflight or responder launch.
                with (
                    mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                    mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="READY_FOR_SPEC")),
                    mock.patch.object(server, "collect_github_work_context") as mock_gh,
                    mock.patch.object(server.asyncio, "create_subprocess_exec") as mock_exec_def,
                ):
                    seed_queued_card(server, "69aaaa222222222222222222", "Spec Card")
                    with self.assertRaises(server.DeferredTriggerRetry):
                        asyncio.run(
                            server.trigger_agent(
                                "69aaaa222222222222222222",
                                "Spec Card",
                                {
                                    "cardId": "69aaaa222222222222222222",
                                    "cardLink": "https://trello.com/c/cardB2",
                                    "listName": "Ready for Spec",
                                    "comment": "Write spec",
                                },
                            )
                        )
                mock_gh.assert_not_called()
                mock_exec_def.assert_not_called()
                defer_state = server.load_sidecar_state()
                self.assertEqual(defer_state["in_flight_cards"]["69aaaa222222222222222222"]["defer_attempts"], 1)
                defer_events = [json.loads(l) for l in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
                self.assertEqual(defer_events[-1]["event"], "trigger_deferred")
                self.assertEqual(defer_events[-1]["reason"], "unverified_card_link")
                self.assertFalse(defer_events[-1]["terminal"])

    def test_trigger_agent_relink_stale_retry_and_timeout_after_tool(self):
        server = load_server()
        with tempfile.TemporaryDirectory() as ws_dir:
            server.WORKSPACES = [ws_dir]
            card_data = {
                "id": "cardXYZ1",
                "shortLink": "cardXYZ1",
                "name": "Test Card",
                "desc": "Card desc",
                "list": {"name": "Ideas"},
                "actions": [
                    {
                        "id": "act-1",
                        "type": "commentCard",
                        "date": "2026-11-19T00:00:00Z",
                        "data": {"text": "Please review"},
                        "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                    }
                ],
            }
            payload = {
                "cardId": "cardXYZ1",
                "cardName": "Test Card",
                "cardLink": "https://trello.com/c/cardXYZ1",
                "comment": "Please review",
                "listName": "Ideas",
            }
            Path(server.SESSION_MAP_FILE).write_text(json.dumps({"cardXYZ1": "stale-conv-0"}))

            # Case 1: failed resume before any tool call retries without --conversation and relinks.
            seen_cmds = []

            async def fake_failed_resume_then_ok(*cmd, **kwargs):
                seen_cmds.append(list(cmd))
                if len(seen_cmds) == 1:
                    lines = [
                        json.dumps({
                            "event": "result",
                            "result": {"conversation_id": "", "status": "ERROR", "error": "conversation corrupt"},
                        })
                    ]
                    return FakeStreamProcess(lines=lines, stderr_bytes=b"conversation corrupt", returncode=1)
                lines = [
                    json.dumps({"event": "init", "conversation_id": "conv-fresh-1"}),
                    json.dumps({
                        "event": "result",
                        "result": {
                            "conversation_id": "conv-fresh-1",
                            "status": "SUCCESS",
                            "response": "Recovered",
                            "usage": {"total_tokens": 90},
                        },
                    }),
                ]
                return FakeStreamProcess(lines=lines, returncode=0)

            seed_queued_card(server, "cardXYZ1", "Test Card")
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=card_data),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_failed_resume_then_ok),
            ):
                asyncio.run(server.trigger_agent("cardXYZ1", "Test Card", dict(payload)))

            self.assertEqual(len(seen_cmds), 2)
            self.assertEqual(seen_cmds[0][seen_cmds[0].index("--conversation") + 1], "stale-conv-0")
            self.assertNotIn("--conversation", seen_cmds[1])
            self.assertEqual(json.loads(Path(server.SESSION_MAP_FILE).read_text())["cardXYZ1"], "conv-fresh-1")

            # Case 2: timeout AFTER init + tool call does NOT retry, preserves init conversation_id, and posts failure only when Trello succeeds.
            seen_cmds.clear()
            posted_failures = []
            hanging_procs = []
            server.AGENT_SUBPROCESS_TIMEOUT_SECONDS = 0.08
            Path(server.SESSION_MAP_FILE).write_text("{}")

            async def fake_hang_exec(*cmd, **kwargs):
                seen_cmds.append(list(cmd))
                proc = FakeStreamProcess(
                    lines=[
                        json.dumps({"event": "init", "conversation_id": "conv-mid-turn"}),
                        json.dumps({"event": "step_update", "step_update": {"step_type": "tool", "tool_name": "run_command"}}),
                    ],
                    hang_after_lines=True,
                )
                hanging_procs.append(proc)
                return proc

            seed_queued_card(server, "cardXYZ1", "Test Card")
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=card_data),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server, "post_trello_comment", side_effect=lambda cid, txt, kind="comment": posted_failures.append((cid, txt, kind)) or False),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_hang_exec),
            ):
                with self.assertRaises(TimeoutError):
                    asyncio.run(server.trigger_agent("cardXYZ1", "Test Card", dict(payload)))

            self.assertEqual(len(seen_cmds), 1)
            self.assertTrue(hanging_procs[0].terminated)
            self.assertEqual(json.loads(Path(server.SESSION_MAP_FILE).read_text())["cardXYZ1"], "conv-mid-turn")
            events = [json.loads(line) for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
            self.assertEqual(events[-1]["event"], "failure_comment_failed")

            # Case 2b: session-map save failure emits conversation_link_failed and no conversation_linked.
            Path(server.SIDECAR_EVENT_LOG_FILE).write_text("")
            Path(server.SESSION_MAP_FILE).write_text("{}")

            async def fake_link_fail_exec(*cmd, **kwargs):
                lines = [
                    json.dumps({"event": "init", "conversation_id": "conv-unpersisted"}),
                    json.dumps({
                        "event": "result",
                        "result": {
                            "conversation_id": "conv-unpersisted",
                            "status": "SUCCESS",
                            "response": "Done",
                            "usage": {"total_tokens": 20},
                        },
                    }),
                ]
                return FakeStreamProcess(lines=lines, returncode=0)

            seed_queued_card(server, "cardXYZ1", "Test Card")
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=card_data),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server, "save_session_mapping", return_value=False),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_link_fail_exec),
            ):
                asyncio.run(server.trigger_agent("cardXYZ1", "Test Card", dict(payload)))

            link_events = [e["event"] for e in (json.loads(l) for l in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines())]
            self.assertIn("conversation_link_failed", link_events)
            self.assertNotIn("conversation_linked", link_events)
            Path(server.SESSION_MAP_FILE).write_text(json.dumps({"cardXYZ1": "conv-mid-turn"}))

            # Case 3: failure comment succeeds -> emits failure_comment_posted and preserves existing session on pre-init error.
            server.AGENT_SUBPROCESS_TIMEOUT_SECONDS = 5
            seen_cmds.clear()

            async def fake_fail_exec(*cmd, **kwargs):
                seen_cmds.append(list(cmd))
                lines = [
                    json.dumps({
                        "event": "result",
                        "result": {"conversation_id": "conv-bad-ignore", "status": "ERROR", "error": "model error"},
                    })
                ]
                return FakeStreamProcess(lines=lines, stderr_bytes=b"model error", returncode=1)

            seed_queued_card(server, "cardXYZ1", "Test Card")
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=card_data),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server, "post_trello_comment", return_value=True),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_fail_exec),
            ):
                with self.assertRaises(RuntimeError):
                    asyncio.run(server.trigger_agent("cardXYZ1", "Test Card", dict(payload)))

            self.assertEqual(len(seen_cmds), 2)
            self.assertEqual(json.loads(Path(server.SESSION_MAP_FILE).read_text())["cardXYZ1"], "conv-mid-turn")
            events = [json.loads(line) for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
            self.assertEqual(events[-1]["event"], "failure_comment_posted")

            # Case 4: finished_reason="print_timeout" with returncode=3 and no tool call does not retry.
            print_timeout_cmds = []

            async def fake_print_timeout_exec(*cmd, **kwargs):
                print_timeout_cmds.append(list(cmd))
                lines = [
                    json.dumps({"event": "init", "conversation_id": "conv-mid-turn"}),
                    json.dumps({
                        "event": "result",
                        "result": {
                            "conversation_id": "conv-mid-turn",
                            "status": "ERROR",
                            "finished_reason": "print_timeout",
                            "error": "agent timed out",
                        },
                    }),
                ]
                return FakeStreamProcess(lines=lines, returncode=3)

            seed_queued_card(server, "cardXYZ1", "Test Card")
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=card_data),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server, "post_trello_comment", return_value=True),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_print_timeout_exec),
            ):
                with self.assertRaises(RuntimeError):
                    asyncio.run(server.trigger_agent("cardXYZ1", "Test Card", dict(payload)))

            self.assertEqual(len(print_timeout_cmds), 1)
            self.assertIn("--conversation", print_timeout_cmds[0])

    def test_classify_action_isolated_sandbox_and_tool_kill(self):
        server = load_server()
        seen_calls = []

        async def fake_ok_classify(*cmd, **kwargs):
            seen_calls.append((list(cmd), dict(kwargs)))
            lines = [
                json.dumps({"event": "init", "conversation_id": "cls-1"}),
                json.dumps({"event": "result", "result": {"status": "SUCCESS", "response": "INVESTIGATOR\n", "usage": {"total_tokens": 50}}}),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        with (
            mock.patch.dict(os.environ, {"TRELLO_API_KEY": "secret-api-key", "RH_SQL_PASSWORD": "db-password-123"}, clear=False),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_ok_classify),
        ):
            phase = asyncio.run(server.classify_action("Look into this new workflow", "Card", "Desc", "Ideas"))

        self.assertEqual(phase, "INVESTIGATOR")
        cmd, kwargs = seen_calls[0]
        self.assertEqual(cmd[cmd.index("--model") + 1], "gemini-pro-orchestrator")
        self.assertIn("--sandbox", cmd)
        self.assertNotIn("--dangerously-skip-permissions", cmd)
        self.assertNotIn("TRELLO_API_KEY", kwargs["env"])
        self.assertNotIn("RH_SQL_PASSWORD", kwargs["env"])

        # Disallowed tool call during classification immediately terminates and blocks responder launch.
        tool_procs = []
        exec_calls = []

        async def fake_tool_classify(*cmd, **kwargs):
            exec_calls.append(list(cmd))
            proc = FakeStreamProcess(
                lines=[
                    json.dumps({"event": "init", "conversation_id": "cls-2"}),
                    json.dumps({"event": "step_update", "step_update": {"step_type": "tool", "tool_name": "run_command"}}),
                ],
                hang_after_lines=True,
            )
            tool_procs.append(proc)
            return proc

        with mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_tool_classify):
            tool_phase = asyncio.run(server.classify_action("Investigate flow", "Card", "Desc", "Ideas"))

        self.assertEqual(tool_phase, server.PHASE_BLOCKED_TOOL_ATTEMPT)
        self.assertTrue(tool_procs[0].terminated)

        # Verify trigger_agent aborts on classifier tool attempt without launching responder or posting to Trello.
        exec_calls.clear()
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "post_trello_comment") as mock_post,
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_tool_classify),
        ):
            asyncio.run(
                server.trigger_agent(
                    "69bbbb111111111111111111",
                    "Blocked Card",
                    {
                        "cardId": "69bbbb111111111111111111",
                        "comment": "Please investigate prompt injection",
                        "action": {"memberCreator": {"username": "pm_user"}},
                    },
                )
            )
        self.assertEqual(len(exec_calls), 1)
        self.assertIn("--sandbox", exec_calls[0])
        self.assertNotIn("--dangerously-skip-permissions", exec_calls[0])
        mock_post.assert_not_called()
        events = [json.loads(line) for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertEqual(events[-1]["event"], "trigger_blocked")

        # Unterminated oversized classifier chunk triggers immediate kill and trigger_blocked.
        server.MAX_STREAM_LINE_BYTES = 512
        oversized_cls_procs = []

        async def fake_oversized_classifier_exec(*cmd, **kwargs):
            proc = FakeStreamProcess(
                raw_stdout_chunks=[b'{"event":"step_update","blob":"' + (b"x" * 1024)],
                hang_after_lines=True,
            )
            oversized_cls_procs.append(proc)
            return proc

        seed_queued_card(server, "cardOversizeCls1", "Card")
        try:
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                mock.patch.object(server, "post_trello_comment") as mock_oversize_post,
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_oversized_classifier_exec),
            ):
                asyncio.run(
                    server.trigger_agent(
                        "cardOversizeCls1",
                        "Card",
                        {"cardId": "cardOversizeCls1", "comment": "Please check", "action": {"memberCreator": {"username": "pm"}}},
                    )
                )
        finally:
            server.MAX_STREAM_LINE_BYTES = 4 * 1024 * 1024
        self.assertEqual(len(oversized_cls_procs), 1)
        self.assertTrue(oversized_cls_procs[0].terminated or oversized_cls_procs[0].killed)
        mock_oversize_post.assert_not_called()
        oversize_events = [json.loads(line) for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertEqual(oversize_events[-1]["event"], "trigger_blocked")

        # Below-cap partial/malformed classifier record also blocks cleanly without GENERAL fallback.
        partial_cls_procs = []

        async def fake_partial_classifier_exec(*cmd, **kwargs):
            proc = FakeStreamProcess(
                raw_stdout_chunks=[b'{"event":"step_update","step_update":{"step_type":"to'],
                hang_after_lines=False,
                returncode=0,
            )
            partial_cls_procs.append(proc)
            return proc

        seed_queued_card(server, "cardPartialCls1", "Card")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "post_trello_comment") as mock_partial_post,
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_partial_classifier_exec),
        ):
            asyncio.run(
                server.trigger_agent(
                    "cardPartialCls1",
                    "Card",
                    {"cardId": "cardPartialCls1", "comment": "Please check", "action": {"memberCreator": {"username": "pm"}}},
                )
            )
        self.assertEqual(len(partial_cls_procs), 1)
        mock_partial_post.assert_not_called()
        partial_events = [json.loads(line) for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertEqual(partial_events[-1]["event"], "trigger_blocked")

        # Partial below-cap classifier bytes left open until timeout ALSO block (never fall back to GENERAL).
        server.CLASSIFIER_TIMEOUT_SECONDS = 0.05
        timeout_partial_procs = []

        async def fake_timeout_partial_classifier(*cmd, **kwargs):
            proc = FakeStreamProcess(
                raw_stdout_chunks=[b'{"event":"step_update","step_update":{"step_type":"to'],
                hang_after_lines=True,
            )
            timeout_partial_procs.append(proc)
            return proc

        seed_queued_card(server, "cardPartialTimeout1", "Card")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "post_trello_comment") as mock_pt_post,
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_timeout_partial_classifier),
        ):
            asyncio.run(
                server.trigger_agent(
                    "cardPartialTimeout1",
                    "Card",
                    {"cardId": "cardPartialTimeout1", "comment": "Please check", "action": {"memberCreator": {"username": "pm"}}},
                )
            )
        self.assertEqual(len(timeout_partial_procs), 1)
        mock_pt_post.assert_not_called()
        pt_events = [json.loads(line) for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertEqual(pt_events[-1]["event"], "trigger_blocked")

        # Invalid UTF-8 bytes inside classifier stream also trigger trigger_blocked.
        bad_utf8_procs = []

        async def fake_bad_utf8_classifier(*cmd, **kwargs):
            proc = FakeStreamProcess(
                raw_stdout_chunks=[b'{"event":"result","result":{"status":"SUCCESS","response":"GENERAL\xff"}}\n'],
                hang_after_lines=False,
                returncode=0,
            )
            bad_utf8_procs.append(proc)
            return proc

        seed_queued_card(server, "cardBadUtf8Cls1", "Card")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "post_trello_comment") as mock_utf8_post,
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_bad_utf8_classifier),
        ):
            asyncio.run(
                server.trigger_agent(
                    "cardBadUtf8Cls1",
                    "Card",
                    {"cardId": "cardBadUtf8Cls1", "comment": "Please check", "action": {"memberCreator": {"username": "pm"}}},
                )
            )
        self.assertEqual(len(bad_utf8_procs), 1)
        mock_utf8_post.assert_not_called()
        utf8_events = [json.loads(line) for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertEqual(utf8_events[-1]["event"], "trigger_blocked")

        # Deeply nested JSON triggering RecursionError in classifier also blocks cleanly (never falls back to GENERAL).
        nested_depth = 12000
        deep_json = (b"[" * nested_depth) + b"1" + (b"]" * nested_depth)
        deep_cls_procs = []

        async def fake_deep_classifier(*cmd, **kwargs):
            proc = FakeStreamProcess(
                raw_stdout_chunks=[b'{"event":"step_update","step_update":{"step_type":"thought","data":' + deep_json + b"}}\n"],
                hang_after_lines=False,
                returncode=0,
            )
            deep_cls_procs.append(proc)
            return proc

        seed_queued_card(server, "cardDeepJsonCls1", "Card")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "post_trello_comment") as mock_deep_post,
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_deep_classifier),
        ):
            asyncio.run(
                server.trigger_agent(
                    "cardDeepJsonCls1",
                    "Card",
                    {"cardId": "cardDeepJsonCls1", "comment": "Please check", "action": {"memberCreator": {"username": "pm"}}},
                )
            )
        self.assertEqual(len(deep_cls_procs), 1)
        mock_deep_post.assert_not_called()
        deep_events = [json.loads(line) for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertEqual(deep_events[-1]["event"], "trigger_blocked")

        # Malformed non-string event values in classifier stream (e.g. {} / []) must block without TypeError fallback to GENERAL.
        self.assertIsNone(server.parse_agy_json_output('{"event":{},"status":"SUCCESS"}'))
        self.assertIsNone(server.parse_agy_json_output('{"event":[],"status":"SUCCESS"}'))
        for idx, bad_event_chunk in enumerate((
            b'{"event":{},"status":"SUCCESS","response":"GENERAL"}\n',
            b'{"event":[],"status":"SUCCESS","response":"GENERAL"}\n',
        )):
            bad_ev_procs = []

            async def fake_bad_ev_classifier(*cmd, chunk=bad_event_chunk, **kwargs):
                proc = FakeStreamProcess(raw_stdout_chunks=[chunk], hang_after_lines=False, returncode=0)
                bad_ev_procs.append(list(cmd))
                return proc

            cid_bad_ev = f"69bbb00000000000000000{idx:02d}"
            seed_queued_card(server, cid_bad_ev, "Bad Event Card")
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                mock.patch.object(server, "post_trello_comment") as mock_bad_ev_post,
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_bad_ev_classifier),
            ):
                asyncio.run(
                    server.trigger_agent(
                        cid_bad_ev,
                        "Bad Event Card",
                        {"cardId": cid_bad_ev, "comment": "Please check", "action": {"memberCreator": {"username": "pm"}}},
                    )
                )
            self.assertEqual(len(bad_ev_procs), 1)
            self.assertIn("--sandbox", bad_ev_procs[0])
            mock_bad_ev_post.assert_not_called()
            ev_logs = [json.loads(line) for line in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
            self.assertEqual(ev_logs[-1]["event"], "trigger_blocked")
            self.assertEqual(ev_logs[-1]["reason"], "classifier_tool_disallowed")

        # Timeout falls back to GENERAL.
        server.CLASSIFIER_TIMEOUT_SECONDS = 0.05
        hanging_procs = []

        async def fake_hang_classify(*cmd, **kwargs):
            proc = FakeStreamProcess(hang_after_lines=True)
            hanging_procs.append(proc)
            return proc

        with mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_hang_classify):
            timed_out_phase = asyncio.run(server.classify_action("Investigate flow", "Card", "Desc", "Ideas"))

        self.assertEqual(timed_out_phase, "GENERAL")
        self.assertTrue(hanging_procs[0].terminated)

    def test_author_resolution_canonical_slot_self_writes_and_http_boundaries(self):
        server = load_server({"TRELLO_SECRET": "trello-app-secret", "TRELLO_WEBHOOK_CALLBACK_URL": "https://example.com/trello-webhook"})
        card_id_long = "691111111111111111111111"
        card_data = {
            "id": card_id_long,
            "shortLink": "RCfxpstT",
            "actions": [
                {
                    "id": "act-human",
                    "type": "commentCard",
                    "date": "2026-11-18T01:00:00Z",
                    "data": {"text": "Hey @**connor** check this"},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                },
                {
                    "id": "act-dup-1",
                    "type": "commentCard",
                    "date": "2026-11-18T01:30:00Z",
                    "data": {"text": "Same comment"},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                },
                {
                    "id": "act-dup-2",
                    "type": "commentCard",
                    "date": "2026-11-18T02:00:00Z",
                    "data": {"text": "Same comment"},
                    "memberCreator": {"username": "trello", "fullName": "Trello Automation"},
                },
                {
                    "id": "act-bot",
                    "type": "commentCard",
                    "date": "2026-11-18T02:30:00Z",
                    "data": {"text": "Automated status ping"},
                    "memberCreator": {"username": "trello", "fullName": "Trello Automation"},
                },
            ],
        }

        matched = server.find_matching_comment_action(card_data, "  Hey @connor check   this ")
        self.assertIsNotNone(matched)
        self.assertEqual(matched["id"], "act-human")
        self.assertIsNone(server.find_matching_comment_action(card_data, ""))
        # Conflicting duplicate text returns None unless action_id is given.
        self.assertIsNone(server.resolve_comment_action(card_data, comment_text="Same comment"))
        exact_dup = server.resolve_comment_action(card_data, comment_text="Same comment", action_id="act-dup-1")
        self.assertEqual(exact_dup["memberCreator"]["username"], "pm_user")

        # Partial raw memberCreator falls through to triggerUserUsername.
        partial_user, _ = server.extract_trigger_member(
            {"action": {"memberCreator": {}}, "triggerUserUsername": "pm_user", "triggerUserName": "PM"}
        )
        self.assertEqual(partial_user, "pm_user")

        server.AUTH_TOKEN = "secret-webhook-token"
        httpd = server.ThreadingHTTPServer(("127.0.0.1", 0), server.WebhookHandler)
        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            with mock.patch.object(server, "fetch_card_details_sync", return_value=card_data):
                # 1. Self-client-identifier header is ignored.
                req_self = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps({"cardLink": "https://trello.com/c/RCfxpstT", "comment": "Hello"}).encode("utf-8"),
                    headers={
                        "Authorization": "Bearer secret-webhook-token",
                        "X-Trello-Client-Identifier": server.TRELLO_CLIENT_IDENTIFIER,
                    },
                )
                with urllib.request.urlopen(req_self, timeout=5) as resp:
                    self.assertEqual(json.loads(resp.read().decode("utf-8")), {"status": "ignored", "reason": "self_client_identifier"})

                # 2. Missing action.memberCreator resolves live author ("trello") and suppresses trigger.
                req_bot = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps(
                        {
                            "cardLink": "https://trello.com/c/RCfxpstT",
                            "cardName": "Demo Card",
                            "comment": "Automated status ping",
                        }
                    ).encode("utf-8"),
                    headers={"Authorization": "Bearer secret-webhook-token", "Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req_bot, timeout=5) as resp:
                    self.assertEqual(json.loads(resp.read().decode("utf-8")), {"status": "ignored", "reason": "suppressed_trigger_member"})

                # 3. Short-link webhook and raw long-ID webhook canonicalize to one slot.
                req_short = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps(
                        {
                            "cardLink": "https://trello.com/c/RCfxpstT",
                            "cardName": "Demo Card",
                            "comment": "Hey @connor check this",
                        }
                    ).encode("utf-8"),
                    headers={"X-Auth-Token": "secret-webhook-token"},
                )
                with urllib.request.urlopen(req_short, timeout=5) as resp:
                    self.assertEqual(resp.status, 202)
                    self.assertEqual(json.loads(resp.read().decode("utf-8")), {"status": "accepted"})

                # Drain queued job while keeping card_id_long pending, then send raw long-ID webhook via X-Trello-Webhook HMAC.
                raw_payload = json.dumps(
                    {
                        "action": {
                            "id": "act-raw-2",
                            "type": "commentCard",
                            "data": {"card": {"id": card_id_long, "shortLink": "RCfxpstT", "name": "Demo Card"}, "text": "Second comment"},
                            "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                        }
                    }
                ).encode("utf-8")
                sig = base64.b64encode(
                    hmac.new(
                        b"trello-app-secret",
                        raw_payload + b"https://example.com/trello-webhook",
                        hashlib.sha1,
                    ).digest()
                ).decode("ascii")
                req_raw = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=raw_payload,
                    headers={"X-Trello-Webhook": sig},
                )
                with urllib.request.urlopen(req_raw, timeout=5) as resp:
                    self.assertEqual(json.loads(resp.read().decode("utf-8")), {"status": "ignored", "reason": "card_already_running"})

                # 4. Non-actionable raw webhook type (removeLabelFromCard) is ignored.
                ignored_raw = json.dumps(
                    {
                        "action": {
                            "id": "act-raw-3",
                            "type": "removeLabelFromCard",
                            "data": {"card": {"id": "692222222222222222222222", "shortLink": "Other123"}},
                            "memberCreator": {"username": "pm_user"},
                        }
                    }
                ).encode("utf-8")
                req_ignored_type = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=ignored_raw,
                    headers={"X-Auth-Token": "secret-webhook-token"},
                )
                with urllib.request.urlopen(req_ignored_type, timeout=5) as resp:
                    self.assertEqual(json.loads(resp.read().decode("utf-8")), {"status": "ignored", "reason": "ignored_action_type"})

                # 5. Non-ASCII auth token returns 401 cleanly (no crash/connection drop).
                bad_auth = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=b"{}",
                    headers={"X-Auth-Token": "t\u00e9st"},
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx_auth:
                    urllib.request.urlopen(bad_auth, timeout=5)
                self.assertEqual(ctx_auth.exception.code, 401)
                ctx_auth.exception.close()

                # 6. Invalid JSON returns 400 cleanly.
                bad_req = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=b"{not-json",
                    headers={"X-Auth-Token": "secret-webhook-token"},
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.urlopen(bad_req, timeout=5)
                self.assertEqual(ctx.exception.code, 400)
                ctx.exception.close()

                # 7. Simulated state persistence failure returns 503 and rolls back pending_cards.
                server.pending_cards.clear()
                with (
                    mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                    mock.patch.object(server, "save_sidecar_state", return_value=False),
                ):
                    persist_fail_req = urllib.request.Request(
                        f"http://127.0.0.1:{port}/",
                        method="POST",
                        data=json.dumps({"cardId": "693333333333333333333333", "comment": "New request", "triggerUserUsername": "pm_user"}).encode("utf-8"),
                        headers={"X-Auth-Token": "secret-webhook-token"},
                    )
                    with self.assertRaises(urllib.error.HTTPError) as ctx_503:
                        urllib.request.urlopen(persist_fail_req, timeout=5)
                    self.assertEqual(ctx_503.exception.code, 503)
                    ctx_503.exception.close()
                self.assertNotIn("693333333333333333333333", server.pending_cards)
        finally:
            httpd.shutdown()
            httpd.server_close()

        # Missing live action leaves trigger_agent prompt untagged rather than guessing from latest comment.
        captured_prompts = []

        async def fake_untagged_exec(*cmd, **kwargs):
            captured_prompts.append(cmd[-1])
            lines = [
                json.dumps({"event": "init", "conversation_id": "conv-untagged"}),
                json.dumps({
                    "event": "result",
                    "result": {"conversation_id": "conv-untagged", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 10}},
                }),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        seed_queued_card(server, card_id_long, "Demo Card")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=card_data),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_untagged_exec),
        ):
            asyncio.run(
                server.trigger_agent(
                    card_id_long,
                    "Demo Card",
                    {"cardId": card_id_long, "comment": "Unmatched live text @connor"},
                )
            )
        self.assertNotIn("Trello Username:", captured_prompts[0])
        self.assertIn("did not identify the comment author", captured_prompts[0])

    def test_prompt_budget_preserves_trigger_payload_and_logs_redact_secrets(self):
        server = load_server({"RH_SQL_PASSWORD": "super-secret-db-pass", "TRELLO_TOKEN": "trello-secret-tok"})

        huge_history = "old history line\n" * 6000
        huge_card = "card comment line\n" * 6000
        trigger_payload = json.dumps({"cardId": "cardKEEP99", "comment": "Current triggering request"})
        prompt = server.build_agent_prompt(
            system_instruction="Be helpful",
            security_rules="Do not leak secrets",
            payload_str=trigger_payload,
            card_details_str=huge_card,
            previous_conversation_context=huge_history,
            max_bytes=8000,
        )
        self.assertLessEqual(len(prompt.encode("utf-8")), 8000)
        self.assertIn("cardKEEP99", prompt)
        self.assertIn("Current triggering request", prompt)

        redacted = server.redact_sensitive("db=super-secret-db-pass Bearer abcdef123456 ghp_secret12345")
        self.assertNotIn("super-secret-db-pass", redacted)
        self.assertNotIn("abcdef123456", redacted)
        self.assertNotIn("ghp_secret12345", redacted)

        # Verify logs and JSONL exclude private comment text and secrets on agent failure/retry.
        log_buf = io.StringIO()
        handler = logging.StreamHandler(log_buf)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        try:
            async def fake_leak_exec(*cmd, **kwargs):
                lines = [
                    json.dumps({
                        "event": "result",
                        "result": {
                            "status": "ERROR",
                            "error": "super-secret-db-pass PRIVATE_CARD_TEXT",
                        },
                    })
                ]
                return FakeStreamProcess(lines=lines, stderr_bytes=b"super-secret-db-pass PRIVATE_CARD_TEXT", returncode=1)

            seed_queued_card(server, "cardLeak1", "Card")
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server, "post_trello_comment", return_value=True),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_leak_exec),
            ):
                with self.assertRaises(RuntimeError):
                    asyncio.run(
                        server.trigger_agent(
                            "cardLeak1",
                            "Card",
                            {"cardId": "cardLeak1", "comment": "PRIVATE_CARD_TEXT", "action": {"memberCreator": {"username": "pm"}}},
                        )
                    )
        finally:
            root_logger.removeHandler(handler)

        log_output = log_buf.getvalue()
        jsonl_output = Path(server.SIDECAR_EVENT_LOG_FILE).read_text()
        self.assertNotIn("super-secret-db-pass", log_output)
        self.assertNotIn("PRIVATE_CARD_TEXT", log_output)
        self.assertNotIn("super-secret-db-pass", jsonl_output)
        self.assertNotIn("PRIVATE_CARD_TEXT", jsonl_output)

    def test_verify_auth_token_and_trello_signature(self):
        server = load_server()
        self.assertTrue(server.verify_auth_token("tok123", "tok123"))
        self.assertTrue(server.verify_auth_token("Bearer tok123", "tok123"))
        self.assertTrue(server.verify_auth_token("bearer tok123", "tok123"))
        self.assertFalse(server.verify_auth_token("wrong", "tok123"))
        self.assertFalse(server.verify_auth_token("t\u00e9st", "tok123"))
        self.assertFalse(server.verify_auth_token("", "tok123"))
        self.assertFalse(server.verify_auth_token("tok123", ""))

        raw = b'{"hello":"world"}'
        cb = "https://example.com/hook"
        sec = "sec123"
        sig = base64.b64encode(hmac.new(sec.encode("utf-8"), raw + cb.encode("utf-8"), hashlib.sha1).digest()).decode("ascii")
        self.assertTrue(server.verify_trello_signature(raw, sig, cb, sec))
        self.assertFalse(server.verify_trello_signature(raw, "bad", cb, sec))

    def test_trello_helper_signature_client_id_and_null_names(self):
        helper = load_helper({"TRELLO_AGENT_SIGNATURE_NAME": "Maeve", "TRELLO_CLIENT_IDENTIFIER": "agy-trello-sidecar"})
        posted = []

        class DummyResp:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b'{"ok":true}'

        def fake_urlopen(req, timeout=10):
            posted.append((req.get_method(), req.headers.get("X-trello-client-identifier"), req.data.decode("utf-8")))
            return DummyResp()

        with mock.patch.object(helper.urllib.request, "urlopen", side_effect=fake_urlopen):
            helper.cmd_comment("card-1", "Here is the update.")

        self.assertEqual(len(posted), 1)
        self.assertEqual(posted[0][0], "POST")
        self.assertEqual(posted[0][1], "agy-trello-sidecar")
        self.assertIn("-+Love+Maeve", posted[0][2])
        self.assertIn("agy-sidecar%3Acomment", posted[0][2])

        responses = [
            ({"idBoard": "board-1"}, None),
            ([{"id": "l1", "name": None}, {"id": "l2", "name": "Ready for Implementation"}], None),
            ({"idBoard": "board-1"}, None),
            ([{"id": "lbl1", "name": None}, {"id": "lbl2", "name": "Ready for Spec"}], None),
        ]
        with mock.patch.object(helper, "make_request", side_effect=responses):
            list_id, list_err = helper.find_list_id("card-1", "Ready for Implementation")
            label_id, label_err = helper.find_label_id("card-1", "Ready for Spec")

        self.assertEqual((list_id, list_err), ("l2", None))
        self.assertEqual((label_id, label_err), ("lbl2", None))

    def test_oversized_stream_records_post_launch_errors_and_malformed_ids(self):
        server = load_server()

        # 1. Oversized record (> 1 MB, < MAX_STREAM_LINE_BYTES) preserves subsequent tool step without retry.
        big_json = json.dumps({"event": "step_update", "step_update": {"step_type": "thought", "text": "a" * (1200 * 1024)}})
        tool_line = json.dumps({"event": "step_update", "step_update": {"step_type": "tool", "tool_name": "run_command"}})
        fail_line = json.dumps({"event": "result", "result": {"conversation_id": "conv-big-1", "status": "ERROR", "error": "boom"}})
        spawned_1 = []

        async def fake_big_valid(*cmd, **kwargs):
            spawned_1.append(list(cmd))
            return FakeStreamProcess(lines=[big_json, tool_line, fail_line], returncode=1)

        seed_queued_card(server, "69cccc111111111111111111", "Big Line")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server, "post_trello_comment", return_value=True),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_big_valid),
        ):
            with self.assertRaises(RuntimeError):
                asyncio.run(server.trigger_agent("69cccc111111111111111111", "Big Line", {"cardId": "69cccc111111111111111111", "comment": "Hi", "action": {"memberCreator": {"username": "pm"}}}))
        self.assertEqual(len(spawned_1), 1)

        # 2. Record exceeding MAX_STREAM_LINE_BYTES sets framing_lost and forbids retry.
        server.MAX_STREAM_LINE_BYTES = 1024
        oversized_line = json.dumps({"event": "step_update", "blob": "z" * 4096})
        spawned_2 = []

        async def fake_framing_lost(*cmd, **kwargs):
            spawned_2.append(list(cmd))
            return FakeStreamProcess(lines=[oversized_line, fail_line], returncode=1)

        seed_queued_card(server, "69cccc222222222222222222", "Lost Framing")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server, "post_trello_comment", return_value=True),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_framing_lost),
        ):
            with self.assertRaises(RuntimeError):
                asyncio.run(server.trigger_agent("69cccc222222222222222222", "Lost Framing", {"cardId": "69cccc222222222222222222", "comment": "Hi", "action": {"memberCreator": {"username": "pm"}}}))
        self.assertEqual(len(spawned_2), 1)
        server.MAX_STREAM_LINE_BYTES = 4 * 1024 * 1024

        # 2b. Below-cap malformed record in responder stream sets framing_lost and forbids retry.
        spawned_2b = []

        async def fake_malformed_responder(*cmd, **kwargs):
            spawned_2b.append(list(cmd))
            return FakeStreamProcess(
                raw_stdout_chunks=[b'{"event":"step_update", bad json\n', fail_line.encode("utf-8") + b"\n"],
                returncode=1,
            )

        seed_queued_card(server, "69cccc2b2b2b2b2b2b2b2b2b", "Malformed Responder")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server, "post_trello_comment", return_value=True),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_malformed_responder),
        ):
            with self.assertRaises(RuntimeError):
                asyncio.run(server.trigger_agent("69cccc2b2b2b2b2b2b2b2b2b", "Malformed Responder", {"cardId": "69cccc2b2b2b2b2b2b2b2b2b", "comment": "Hi", "action": {"memberCreator": {"username": "pm"}}}))
        self.assertEqual(len(spawned_2b), 1)

        # 2c. Malformed step_update envelope in responder stream (missing step_type / empty object) sets framing_lost and forbids retry,
        # while valid non-tool step_update and legacy flat result records still succeed cleanly.
        for bad_envelope in (
            b'{"event":"step_update","step_update":{}}\n',
            b'{"event":"step_update"}\n',
            b'{"event":"step_update","step_update":{"step_type":{"bad":1}}}\n',
        ):
            spawned_2c = []

            async def fake_bad_step_env(*cmd, env_bytes=bad_envelope, **kwargs):
                spawned_2c.append(list(cmd))
                return FakeStreamProcess(
                    raw_stdout_chunks=[env_bytes, fail_line.encode("utf-8") + b"\n"],
                    returncode=1,
                )

            cid_2c = f"69cccc2c{len(bad_envelope):016x}"[:24]
            seed_queued_card(server, cid_2c, "Malformed Envelope")
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server, "post_trello_comment", return_value=True),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_bad_step_env),
            ):
                with self.assertRaises(RuntimeError):
                    asyncio.run(server.trigger_agent(cid_2c, "Malformed Envelope", {"cardId": cid_2c, "comment": "Hi", "action": {"memberCreator": {"username": "pm"}}}))
            self.assertEqual(len(spawned_2c), 1)

        # Valid non-tool step_update followed by legacy flat result succeeds cleanly.
        async def fake_valid_step_flat(*cmd, **kwargs):
            return FakeStreamProcess(
                lines=[
                    json.dumps({"event": "step_update", "step_update": {"step_type": "thought", "text": "Reasoning"}}),
                    json.dumps({"status": "SUCCESS", "conversation_id": "conv-flat-valid", "response": "Done", "usage": {"total_tokens": 5}}),
                ],
                returncode=0,
            )

        seed_queued_card(server, "69cccc2d2d2d2d2d2d2d2d2d", "Valid Step Flat Result")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_valid_step_flat),
        ):
            asyncio.run(server.trigger_agent("69cccc2d2d2d2d2d2d2d2d2d", "Valid Step Flat Result", {"cardId": "69cccc2d2d2d2d2d2d2d2d2d", "comment": "Hi", "action": {"memberCreator": {"username": "pm"}}}))
        self.assertEqual(server.load_session_mapping().get("69cccc2d2d2d2d2d2d2d2d2d"), "conv-flat-valid")

        # 3. Post-launch exception inside stream consumption never retries and terminates child.
        spawned_3 = []

        async def fake_post_launch_exc(*cmd, **kwargs):
            proc = FakeStreamProcess(hang_after_lines=True)
            spawned_3.append(proc)
            return proc

        seed_queued_card(server, "69cccc333333333333333333", "Post Exc")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server, "post_trello_comment", return_value=True),
            mock.patch.object(server, "consume_agy_stream", side_effect=ValueError("stream exploded")),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_post_launch_exc),
        ):
            with self.assertRaises(ValueError):
                asyncio.run(server.trigger_agent("69cccc333333333333333333", "Post Exc", {"cardId": "69cccc333333333333333333", "comment": "Hi", "action": {"memberCreator": {"username": "pm"}}}))
        self.assertEqual(len(spawned_3), 1)
        self.assertTrue(spawned_3[0].terminated)

        # 4. Malformed init and final conversation IDs neither persist aliases nor emit agent_run_completed.
        Path(server.SIDECAR_EVENT_LOG_FILE).write_text("")
        Path(server.SESSION_MAP_FILE).write_text("{}")

        async def fake_malformed_id_exec(*cmd, **kwargs):
            lines = [
                json.dumps({"event": "init", "conversation_id": {"bad": 1}}),
                json.dumps({"event": "result", "result": {"conversation_id": 12345, "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        seed_queued_card(server, "69cccc444444444444444444", "Bad CID")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server, "post_trello_comment", return_value=True),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_malformed_id_exec),
        ):
            with self.assertRaises(RuntimeError):
                asyncio.run(server.trigger_agent("69cccc444444444444444444", "Bad CID", {"cardId": "69cccc444444444444444444", "comment": "Hi", "action": {"memberCreator": {"username": "pm"}}}))
        self.assertEqual(json.loads(Path(server.SESSION_MAP_FILE).read_text()), {})
        bad_events = [json.loads(l)["event"] for l in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertNotIn("agent_run_completed", bad_events)

    def test_exited_leader_descendant_process_group_cleanup(self):
        server = load_server()

        async def _run():
            proc = FakeStreamProcess(
                lines=[json.dumps({"event": "init", "conversation_id": "conv-pgid-1"})],
                hang_after_lines=True,
                pid=42424,
                force_returncode=0,
            )
            pgid_alive = True
            sent_signals = []

            def fake_killpg(pgid, sig):
                nonlocal pgid_alive
                if pgid != 42424:
                    raise AssertionError(f"Unexpected pgid signaled: {pgid}")
                if sig == 0:
                    if not pgid_alive:
                        raise ProcessLookupError()
                    return None
                sent_signals.append((pgid, sig))
                if sig in (signal.SIGTERM, signal.SIGKILL):
                    pgid_alive = False
                    proc.stdout.feed_eof()
                    proc.stderr.feed_eof()

            with mock.patch.object(server.os, "killpg", side_effect=fake_killpg):
                server.register_active_process(proc)
                res = await server.consume_agy_stream(proc, timeout_seconds=0.05)
                server.unregister_active_process(proc)
            return res, sent_signals

        res, sent_signals = asyncio.run(_run())
        self.assertTrue(res["timed_out"])
        self.assertIn((42424, signal.SIGTERM), sent_signals)

    def test_http_bounds_canonicalization_raw_allowlist_and_post_admission_recheck(self):
        server = load_server({"TRELLO_SECRET": "trello-app-secret", "TRELLO_WEBHOOK_CALLBACK_URL": "https://example.com/trello-webhook"})
        httpd = server.BoundedThreadingHTTPServer(("127.0.0.1", 0), server.WebhookHandler)
        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            # 1. Signed oversized payload (> MAX_WEBHOOK_BODY_BYTES) returns 413.
            server.MAX_WEBHOOK_BODY_BYTES = 128
            req_large = urllib.request.Request(
                f"http://127.0.0.1:{port}/",
                method="POST",
                data=b"x" * 256,
                headers={"X-Trello-Webhook": "any-sig"},
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx_413:
                urllib.request.urlopen(req_large, timeout=5)
            self.assertEqual(ctx_413.exception.code, 413)
            ctx_413.exception.close()
            server.MAX_WEBHOOK_BODY_BYTES = 1024 * 1024

            # 2. Incomplete body returns 400.
            with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
                sock.sendall(
                    b"POST / HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Auth-Token: test-webhook-token\r\n"
                    b"Content-Length: 100\r\n\r\n{}"
                )
                sock.shutdown(socket.SHUT_WR)
                raw_resp = sock.recv(4096)
            self.assertIn(b"400", raw_resp.splitlines()[0])

            # 3. Stalled incomplete body returns 408 on read deadline.
            prev_read_timeout = server.WEBHOOK_READ_TIMEOUT_SECONDS
            server.WEBHOOK_READ_TIMEOUT_SECONDS = 0.15
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
                    sock.sendall(
                        b"POST / HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Auth-Token: test-webhook-token\r\n"
                        b"Content-Length: 100\r\n\r\n{"
                    )
                    sock.settimeout(3)
                    raw_408 = sock.recv(4096)
                self.assertIn(b"408", raw_408.splitlines()[0])
            finally:
                server.WEBHOOK_READ_TIMEOUT_SECONDS = prev_read_timeout

            # Saturated worker semaphore returns 503 immediately instead of spawning unbounded threads.
            busy_srv = server.BoundedThreadingHTTPServer(("127.0.0.1", 0), server.WebhookHandler, max_workers=1)
            busy_port = busy_srv.server_address[1]
            busy_thread = threading.Thread(target=busy_srv.serve_forever, daemon=True)
            busy_thread.start()
            try:
                self.assertTrue(busy_srv._worker_semaphore.acquire(blocking=False))
                try:
                    req_busy = urllib.request.Request(f"http://127.0.0.1:{busy_port}/", method="HEAD")
                    with self.assertRaises(urllib.error.HTTPError) as ctx_busy:
                        urllib.request.urlopen(req_busy, timeout=5)
                    self.assertEqual(ctx_busy.exception.code, 503)
                    ctx_busy.exception.close()
                finally:
                    busy_srv._worker_semaphore.release()
            finally:
                busy_srv.shutdown()
                busy_srv.server_close()

            # 4. Signed board-level raw event without a card is ignored and queues nothing.
            board_ev = json.dumps({
                "action": {
                    "id": "act-board-1",
                    "type": "updateBoard",
                    "data": {"board": {"id": "board-1", "name": "Product"}},
                    "memberCreator": {"username": "pm_user"},
                }
            }).encode("utf-8")
            sig_board = base64.b64encode(
                hmac.new(b"trello-app-secret", board_ev + b"https://example.com/trello-webhook", hashlib.sha1).digest()
            ).decode("ascii")
            req_board = urllib.request.Request(
                f"http://127.0.0.1:{port}/",
                method="POST",
                data=board_ev,
                headers={"X-Trello-Webhook": sig_board},
            )
            with urllib.request.urlopen(req_board, timeout=5) as resp:
                self.assertEqual(json.loads(resp.read().decode("utf-8")), {"status": "ignored", "reason": "ignored_action_type"})
            self.assertTrue(server.task_queue.empty())

            # 5. Failed short-ID canonicalization returns 503 and prevents dual short/long slots.
            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                req_short_fail = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps({"cardLink": "https://trello.com/c/ShortFail1", "comment": "Hello"}).encode("utf-8"),
                    headers={"X-Auth-Token": "test-webhook-token"},
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx_short_503:
                    urllib.request.urlopen(req_short_fail, timeout=5)
                self.assertEqual(ctx_short_503.exception.code, 503)
                ctx_short_503.exception.close()
                self.assertTrue(server.task_queue.empty())

                # Cached verified alias resolves canonical ID even if live lookup transiently returns None.
                server.record_verified_card_alias("CachedShort1", "69ffff111111111111111111")
                req_cached_short = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps({
                        "cardLink": "https://trello.com/c/CachedShort1",
                        "comment": "Followup on cached card",
                        "triggerUserUsername": "pm_user",
                    }).encode("utf-8"),
                    headers={"X-Auth-Token": "test-webhook-token"},
                )
                with urllib.request.urlopen(req_cached_short, timeout=5) as resp:
                    self.assertEqual(resp.status, 202)
                cached_key, _, _ = server.task_queue.get_nowait()
                self.assertEqual(cached_key, "69ffff111111111111111111")
                server.release_pending_card(cached_key)

                # Long-ID follow-up succeeds and leaves exactly one queued job; omitted author is caught later by worker recheck.
                req_long_ok = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps({"cardId": "69dddd111111111111111111", "comment": "Automated bot ping without author"}).encode("utf-8"),
                    headers={"X-Auth-Token": "test-webhook-token"},
                )
                with urllib.request.urlopen(req_long_ok, timeout=5) as resp:
                    self.assertEqual(resp.status, 202)
            self.assertEqual(server.task_queue.qsize(), 1)
            queued_key, queued_name, queued_payload = server.task_queue.get_nowait()

            # Worker resolves live author as suppressed user ("trello") and exits cleanly without spawning Agy or posting.
            bot_card = {
                "id": "69dddd111111111111111111",
                "shortLink": "ShortBot1",
                "name": "Bot Card",
                "desc": "Desc",
                "list": {"name": "Ideas"},
                "actions": [
                    {
                        "id": "act-bot-recheck",
                        "type": "commentCard",
                        "date": "2026-11-19T01:00:00Z",
                        "data": {"text": "Automated bot ping without author"},
                        "memberCreator": {"username": "trello", "fullName": "Trello"},
                    }
                ],
            }
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=bot_card),
                mock.patch.object(server, "post_trello_comment") as mock_post,
                mock.patch.object(server.asyncio, "create_subprocess_exec") as mock_exec,
            ):
                asyncio.run(server.trigger_agent(queued_key, queued_name, queued_payload))
            mock_exec.assert_not_called()
            mock_post.assert_not_called()
            recheck_events = [json.loads(l) for l in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
            self.assertEqual(recheck_events[-1]["event"], "trigger_ignored")
            self.assertEqual(recheck_events[-1]["reason"], "suppressed_trigger_member")
        finally:
            httpd.shutdown()
            httpd.server_close()

    def test_non_comment_move_budget_priority_and_ack_persistence_failure(self):
        server = load_server({"TRELLO_POST_ACK_COMMENTS": "true"})

        # 1. Non-comment card move never substitutes the latest historical comment into classify_action or Triggering User mentions.
        move_card_data = {
            "id": "69eeee111111111111111111",
            "shortLink": "MoveCard1",
            "name": "Move Card",
            "desc": "Card desc",
            "list": {"name": "In Progress"},
            "actions": [
                {
                    "id": "act-old-spec",
                    "type": "commentCard",
                    "date": "2026-11-19T02:00:00Z",
                    "data": {"text": "Requirements are locked in, please write a spec @designer"},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                }
            ],
        }
        seen_classify_args = []
        captured_prompts = []

        async def fake_cls(comment, card_name, card_desc, list_name, action_type=None):
            seen_classify_args.append((comment, action_type))
            return "GENERAL"

        async def fake_move_exec(*cmd, **kwargs):
            captured_prompts.append(cmd[-1])
            lines = [
                json.dumps({"event": "init", "conversation_id": "conv-move-1"}),
                json.dumps({"event": "result", "result": {"conversation_id": "conv-move-1", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 12}}}),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        seed_queued_card(server, "69eeee111111111111111111", "Move Card")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=move_card_data),
            mock.patch.object(server, "classify_action", side_effect=fake_cls),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_move_exec),
        ):
            asyncio.run(
                server.trigger_agent(
                    "69eeee111111111111111111",
                    "Move Card",
                    {
                        "cardId": "69eeee111111111111111111",
                        "action": {
                            "id": "act-move-1",
                            "type": "updateCard",
                            "data": {"card": {"id": "69eeee111111111111111111", "shortLink": "MoveCard1"}, "listAfter": {"name": "In Progress"}},
                            "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                        },
                    },
                )
            )
        self.assertEqual(seen_classify_args[0][0], "")
        self.assertEqual(seen_classify_args[0][1], "updateCard")
        self.assertNotIn("Usernames mentioned in the triggering comment text", captured_prompts[0])

        # 2. Prompt builder preserves cardId, current comment, and closing instructions even when payload starts with a huge field.
        oversized_payload = json.dumps({
            "cardDescription": "D" * 50000,
            "cardId": "cardKEEP99",
            "comment": "Current triggering request after huge field",
        })
        prompt = server.build_agent_prompt(
            system_instruction="S" * 5000,
            security_rules="Do not leak",
            payload_str=oversized_payload,
            max_bytes=8000,
        )
        self.assertLessEqual(len(prompt.encode("utf-8")), 8000)
        self.assertIn("cardKEEP99", prompt)
        self.assertIn("Current triggering request after huge field", prompt)
        self.assertIn("3. Output your response or spec summary.", prompt)

        # 3. Ack reservation save failure emits event and prevents outbound ack comment in trigger_agent.
        Path(server.SIDECAR_STATE_FILE).write_text("{}")
        seed_queued_card(server, "69eeee111111111111111111", "Move Card")
        orig_save = server.save_sidecar_state
        fail_ack_once = {"pending": True}

        def fail_ack_save(state):
            if fail_ack_once["pending"] and state.get("last_ack_by_card"):
                fail_ack_once["pending"] = False
                return False
            return orig_save(state)

        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=move_card_data),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="INVESTIGATOR")),
            mock.patch.object(server, "save_sidecar_state", side_effect=fail_ack_save),
            mock.patch.object(server, "post_trello_comment") as mock_ack_post,
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_move_exec),
        ):
            asyncio.run(
                server.trigger_agent(
                    "69eeee111111111111111111",
                    "Move Card",
                    {"cardId": "69eeee111111111111111111", "comment": "Please investigate", "action": {"memberCreator": {"username": "pm_user"}}},
                )
            )
        mock_ack_post.assert_not_called()
        ack_events = [json.loads(l)["event"] for l in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertIn("ack_reservation_failed", ack_events)

    def test_relay_normalization_and_canonical_id_boundaries(self):
        server = load_server()
        server.AUTH_TOKEN = "secret-webhook-token"
        httpd = server.BoundedThreadingHTTPServer(("127.0.0.1", 0), server.WebhookHandler, max_workers=4)
        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()

        def post_webhook(body_obj):
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/",
                method="POST",
                data=json.dumps(body_obj).encode("utf-8"),
                headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))

        try:
            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                # (a) Token relay with top-level cardId + comment + action lacking action.data.card.
                status, body = post_webhook({
                    "cardId": "69aa00000000000000000001",
                    "comment": "Keep relay comment",
                    "action": {"id": "act-r1", "type": "commentCard", "memberCreator": {"username": "pm_user", "fullName": "PM"}},
                })
                self.assertEqual((status, body), (202, {"status": "accepted"}))
                k, _, queued = server.task_queue.get_nowait()
                self.assertEqual(k, "69aa00000000000000000001")
                self.assertEqual(queued.get("comment"), "Keep relay comment")
                server.release_pending_card(k)

                # (b) Token relay with action.data.card + empty action.data.text falls back to top-level comment.
                status, body = post_webhook({
                    "cardId": "69aa00000000000000000002",
                    "comment": "Fallback top comment",
                    "action": {
                        "id": "act-r2",
                        "type": "commentCard",
                        "data": {"card": {"id": "69aa00000000000000000002", "name": "Card B"}, "text": ""},
                        "memberCreator": {"username": "pm_user"},
                    },
                })
                self.assertEqual((status, body), (202, {"status": "accepted"}))
                k, _, queued = server.task_queue.get_nowait()
                self.assertEqual(queued.get("comment"), "Fallback top comment")
                server.release_pending_card(k)

                # (c) Simplified relay with actionType="updateCard" strips stale top-level comment.
                status, body = post_webhook({
                    "cardId": "69aa00000000000000000003",
                    "actionType": "updateCard",
                    "listAfter": "In Progress",
                    "comment": "Stale comment from previous event",
                    "triggerUserUsername": "pm_user",
                })
                self.assertEqual((status, body), (202, {"status": "accepted"}))
                k, _, queued = server.task_queue.get_nowait()
                self.assertNotIn("comment", queued)
                server.release_pending_card(k)

                # (d) Short cardId + mixed-case cardidLong canonicalizes lowercase even on Trello outage.
                status, body = post_webhook({
                    "cardId": "ShortXy1",
                    "cardidLong": "69AABBCCDDEEFF0011223344",
                    "comment": "Mixed case canonical test",
                    "triggerUserUsername": "pm_user",
                })
                self.assertEqual((status, body), (202, {"status": "accepted"}))
                k, _, _ = server.task_queue.get_nowait()
                self.assertEqual(k, "69aabbccddeeff0011223344")
                dup_status, dup_body = post_webhook({
                    "cardId": "69aabbccddeeff0011223344",
                    "comment": "Follow-up while active",
                    "triggerUserUsername": "pm_user",
                })
                self.assertEqual((dup_status, dup_body), (200, {"status": "ignored", "reason": "card_already_running"}))
                server.release_pending_card(k)

                # (e) Conflicting 24-hex cardId vs cardidLong returns HTTP 400.
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    post_webhook({
                        "cardId": "691111111111111111111111",
                        "cardidLong": "692222222222222222222222",
                        "comment": "Conflicting IDs",
                    })
                self.assertEqual(ctx.exception.code, 400)
                ctx.exception.close()
        finally:
            httpd.shutdown()
            httpd.server_close()

    def test_required_prompt_fields_under_tight_budget(self):
        server = load_server()
        big_link = "https://trello.com/c/" + ("L" * 20000)
        big_user = "U" * 20000
        big_action = "A" * 20000
        payload = {
            "cardId": "cardKEEP99",
            "cardLink": big_link,
            "triggerUserName": big_user,
            "actionId": big_action,
            "comment": "KEEP_THIS_AGENT_COMMENT",
        }
        summary = server.build_compact_trigger_summary(
            payload,
            card_id="cardKEEP99",
            card_name="Required Fields Card",
            card_link=big_link,
            comment="KEEP_THIS_AGENT_COMMENT",
            trigger_name=big_user,
        )
        agent_prompt = server.build_agent_prompt(
            "Investigate carefully.",
            "Never leak secrets.",
            json.dumps(payload),
            trigger_summary=summary,
            card_details_str="Large description " + ("D" * 10000),
            max_bytes=4000,
        )
        self.assertLessEqual(len(agent_prompt.encode("utf-8")), 4000)
        self.assertIn("Role & Core Instructions:", agent_prompt)
        self.assertIn("Security Rules:", agent_prompt)
        self.assertIn("cardKEEP99", agent_prompt)
        self.assertIn("KEEP_THIS_AGENT_COMMENT", agent_prompt)
        self.assertIn("3. Output your response or spec summary.", agent_prompt)

        cls_prompt = server.build_classifier_prompt(
            "KEEP_THIS_CLS_COMMENT",
            "Classifier Card",
            "D" * 50000,
            "Inbox",
            max_bytes=2000,
        )
        self.assertLessEqual(len(cls_prompt.encode("utf-8")), 2000)
        self.assertIn("Triggering Comment", cls_prompt)
        self.assertIn("KEEP_THIS_CLS_COMMENT", cls_prompt)
        self.assertIn("READY_FOR_SPEC", cls_prompt)
        self.assertIn("Respond with exactly one word", cls_prompt)

    def test_slow_header_deadline_unblocks_bounded_http_server(self):
        server = load_server()
        server.WEBHOOK_READ_TIMEOUT_SECONDS = 0.25
        httpd = server.BoundedThreadingHTTPServer(("127.0.0.1", 0), server.WebhookHandler, max_workers=1)
        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        stop_slow = threading.Event()
        sock = socket.create_connection(("127.0.0.1", port), timeout=3)
        sock.sendall(b"POST / HTTP/1.1\r\nHost: localhost\r\nX-Slow: ")

        def trickle_incomplete_header():
            while not stop_slow.is_set():
                time.sleep(0.05)
                try:
                    sock.sendall(b"a")
                except (BrokenPipeError, ConnectionResetError, OSError):
                    break

        trickle_thread = threading.Thread(target=trickle_incomplete_header, daemon=True)
        trickle_thread.start()
        try:
            time.sleep(0.4)
            # Slow client remains open while a second connection claims the single worker slot cleanly.
            head_req = urllib.request.Request(f"http://127.0.0.1:{port}/", method="HEAD")
            with urllib.request.urlopen(head_req, timeout=1.5) as resp:
                self.assertEqual(resp.status, 200)
        finally:
            stop_slow.set()
            trickle_thread.join(timeout=1.0)
            sock.close()
            httpd.shutdown()
            httpd.server_close()

    def test_shutdown_gate_aborts_before_launch_and_preserves_queued_row(self):
        server = load_server()
        card_key = "69dddd111111111111111111"
        payload = {"cardId": card_key, "comment": "Hello"}
        seed_queued_card(server, card_key, "Shutdown Card", payload=payload)

        def shutdown_on_fetch(*args, **kwargs):
            server.initiate_shutdown()
            return None

        with (
            mock.patch.object(server, "fetch_card_details_sync", side_effect=shutdown_on_fetch),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server.asyncio, "create_subprocess_exec") as mock_exec,
        ):
            server.run_agent_job(card_key, "Shutdown Card", payload)

        mock_exec.assert_not_called()
        state = server.load_sidecar_state()
        self.assertEqual(state["in_flight_cards"][card_key]["stage"], "queued")

        # Mixed-case legacy queued key migrates cleanly to lowercase on disk upon requeue.
        server.shutdown_event.clear()
        server.pending_cards.clear()
        while not server.task_queue.empty():
            server.task_queue.get_nowait()
        legacy_key = "69DDEEFF0011223344556677"
        lower_key = legacy_key.lower()
        Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({
            "in_flight_cards": {
                legacy_key: {
                    "card_name": "Legacy Card",
                    "payload": {"cardId": legacy_key, "comment": "Hi"},
                    "queued_at": time.time(),
                    "stage": "queued",
                }
            }
        }))
        self.assertTrue(server.requeue_in_flight_cards())
        migrated_state = server.load_sidecar_state()
        self.assertNotIn(legacy_key, migrated_state["in_flight_cards"])
        self.assertIn(lower_key, migrated_state["in_flight_cards"])
        queued_key, _, queued_payload = server.task_queue.get_nowait()
        self.assertEqual(queued_key, lower_key)
        self.assertFalse(server.mark_in_flight_dispatched(lower_key, admission_id="wrong-id"))
        self.assertTrue(server.mark_in_flight_dispatched(lower_key, admission_id=queued_payload["_admission_id"]))

    def test_planner_single_verified_card_projection_across_branches(self):
        server = load_server()
        server.AUTH_TOKEN = "secret-webhook-token"
        card_a_id = "69aaaa111111111111111111"
        card_a_live = {
            "id": card_a_id,
            "shortLink": "cardA111",
            "url": "https://trello.com/c/cardA111",
            "name": "Card Alpha",
            "desc": "Build feature cleanly",
            "list": {"name": "Ready for Spec"},
            "actions": [
                {
                    "id": "act-proj-1",
                    "type": "commentCard",
                    "date": "2026-11-19T00:00:00Z",
                    "data": {"text": "Write spec now"},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                }
            ],
        }
        spoofed_body = {
            "cardId": card_a_id,
            "cardShortId": "staleCardB",
            "cardLink": "https://trello.com/c/staleCardB",
            "shortLink": "staleTopShort",
            "shortUrl": "https://trello.com/c/staleTopUrl",
            "comment": "Write spec now",
            "listName": "Ready for Spec",
            "action": {
                "id": "act-proj-1",
                "type": "commentCard",
                "data": {
                    "text": "Write spec now",
                    "board": {"id": "board-1", "name": "Product Board", "shortLink": "brd1"},
                    "card": {
                        "id": card_a_id,
                        "shortLink": "staleCardB",
                        "shortUrl": "https://trello.com/c/staleCardB",
                        "url": "https://trello.com/c/staleCardB/stale-title",
                    },
                },
                "memberCreator": {"username": "pm_user", "fullName": "PM User"},
            },
            "model": {
                "id": "69bbbbbbbbbbbbbbbbbbbbbb",
                "url": "https://trello.com/c/staleUrlOnly/old-snapshot",
            },
        }

        preflight_urls = []
        captured_prompts = []

        async def fake_planner_exec(*cmd, **kwargs):
            captured_prompts.append(cmd[-1])
            lines = [
                json.dumps({"event": "init", "conversation_id": "conv-proj-1"}),
                json.dumps({"event": "result", "result": {"conversation_id": "conv-proj-1", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        def fake_preflight(cname, clink):
            preflight_urls.append(clink)
            return ""

        httpd = server.BoundedThreadingHTTPServer(("127.0.0.1", 0), server.WebhookHandler, max_workers=4)
        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            # (a) Live verification rewrites nested & top-level identity to cardA111 via HTTP -> worker.
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=card_a_live),
                mock.patch.object(server, "collect_github_work_context", side_effect=fake_preflight),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_planner_exec),
            ):
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps(spoofed_body).encode("utf-8"),
                    headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    self.assertEqual(resp.status, 202)
                q_key, q_name, q_payload = server.task_queue.get_nowait()
                server.run_agent_job(q_key, q_name, q_payload)

            self.assertEqual(preflight_urls[-1], "https://trello.com/c/cardA111")
            self.assertIn("https://trello.com/c/cardA111", captured_prompts[-1])
            self.assertIn("Product Board", captured_prompts[-1])
            self.assertNotIn("staleCardB", captured_prompts[-1])
            self.assertNotIn("staleTopShort", captured_prompts[-1])
            self.assertNotIn("staleTopUrl", captured_prompts[-1])
            self.assertNotIn("staleUrlOnly", captured_prompts[-1])

            # (b) Cache-hit outage (distinct action) projects cached cardA111 over stale nested links too.
            spoofed_body_2 = copy.deepcopy(spoofed_body)
            spoofed_body_2["comment"] = "Write updated spec now"
            spoofed_body_2["action"]["id"] = "act-proj-2"
            spoofed_body_2["action"]["data"]["text"] = "Write updated spec now"
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                mock.patch.object(server, "collect_github_work_context", side_effect=fake_preflight),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_planner_exec),
            ):
                req2 = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps(spoofed_body_2).encode("utf-8"),
                    headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req2, timeout=5) as resp2:
                    self.assertEqual(resp2.status, 202)
                q_key, q_name, q_payload = server.task_queue.get_nowait()
                server.run_agent_job(q_key, q_name, q_payload)

            self.assertEqual(preflight_urls[-1], "https://trello.com/c/cardA111")
            self.assertIn("https://trello.com/c/cardA111", captured_prompts[-1])
            self.assertIn("Product Board", captured_prompts[-1])
            self.assertNotIn("staleCardB", captured_prompts[-1])
            self.assertNotIn("staleTopShort", captured_prompts[-1])
            self.assertNotIn("staleTopUrl", captured_prompts[-1])
            self.assertNotIn("staleUrlOnly", captured_prompts[-1])

            # (c) Live card response lacking shortLink & url strips all stale nested links in prompt.
            card_no_link = {
                "id": "69aaaa222222222222222222",
                "name": "No Link Card",
                "desc": "Discussion",
                "list": {"name": "General"},
                "actions": [],
            }
            spoofed_no_link = copy.deepcopy(spoofed_body)
            spoofed_no_link["cardId"] = "69aaaa222222222222222222"
            spoofed_no_link["listName"] = "General"
            spoofed_no_link["action"]["id"] = "act-proj-3"
            spoofed_no_link["action"]["data"]["card"]["id"] = "69aaaa222222222222222222"
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=card_no_link),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_planner_exec),
            ):
                seed_queued_card(server, "69aaaa222222222222222222", "No Link Card", payload=spoofed_no_link)
                server.run_agent_job("69aaaa222222222222222222", "No Link Card", spoofed_no_link)
            self.assertNotIn("staleCardB", captured_prompts[-1])
            self.assertNotIn("staleUrlOnly", captured_prompts[-1])
            self.assertNotIn("staleTopUrl", captured_prompts[-1])
            self.assertNotIn("staleTopShort", captured_prompts[-1])
            self.assertIn("Product Board", captured_prompts[-1])

            # (d) Object-valued nested/promoted metadata is omitted while valid scalar board metadata stays.
            obj_meta_body = {
                "cardId": card_a_id,
                "listName": {"cardUrl": "https://trello.com/c/evilTopList"},
                "comment": "Check object metadata handling",
                "action": {
                    "id": "act-proj-obj-meta",
                    "type": "updateCard",
                    "data": {
                        "board": {"id": "board-1", "name": {"evil": "https://trello.com/c/evilBoardObj"}, "shortLink": "brd1"},
                        "listAfter": {"name": {"evil": "https://trello.com/c/evilListAfter"}},
                        "label": {"name": {"evil": "https://trello.com/c/evilLabel"}},
                        "card": {"id": card_a_id},
                    },
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                },
            }
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=card_a_live),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_planner_exec),
            ):
                req_obj = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps(obj_meta_body).encode("utf-8"),
                    headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req_obj, timeout=5) as resp_obj:
                    self.assertEqual(resp_obj.status, 202)
                qk_obj, qn_obj, qp_obj = server.task_queue.get_nowait()
                self.assertNotIsInstance(qp_obj.get("listName"), dict)
                server.run_agent_job(qk_obj, qn_obj, qp_obj)

            last_prompt = captured_prompts[-1]
            self.assertIn("brd1", last_prompt)
            self.assertIn("board-1", last_prompt)
            self.assertNotIn("evilTopList", last_prompt)
            self.assertNotIn("evilBoardObj", last_prompt)
            self.assertNotIn("evilListAfter", last_prompt)
            self.assertNotIn("evilLabel", last_prompt)

            # Direct scalar board name check on project_prompt_payload preserves scalar fields and omits non-string fields.
            projected_clean = server.project_prompt_payload(
                {
                    "cardId": card_a_id,
                    "action": {
                        "type": "updateCard",
                        "data": {
                            "board": {"id": "board-2", "name": "Scalar Board", "shortLink": "brd2"},
                            "listAfter": {"name": {"bad": 1}},
                            "label": {"name": "Urgent"},
                        },
                    },
                },
                canonical_id=card_a_id,
            )
            self.assertEqual(projected_clean["board"], {"id": "board-2", "name": "Scalar Board", "shortLink": "brd2"})
            self.assertNotIn("listAfter", projected_clean)
            self.assertEqual(projected_clean.get("label"), "Urgent")

            # (e) Unverified stale short link on a canonical card row never poisons the other card's shared delivery key.
            card_iso_a = "69aa79000000000000000011"
            card_iso_b = "69aa79000000000000000022"
            live_iso_a = {
                "id": card_iso_a,
                "shortLink": "RealA111",
                "url": "https://trello.com/c/RealA111",
                "name": "Iso A",
                "desc": "Desc A",
                "list": {"name": "Ideas"},
                "actions": [],
            }
            live_iso_b = {
                "id": card_iso_b,
                "shortLink": "StaleB22",
                "url": "https://trello.com/c/StaleB22",
                "name": "Iso B",
                "desc": "Desc B",
                "list": {"name": "Ideas"},
                "actions": [],
            }
            spoof_iso_a = {
                "cardId": card_iso_a,
                "cardShortId": "StaleB22",
                "cardLink": "https://trello.com/c/StaleB22",
                "action": {
                    "type": "addLabelToCard",
                    "data": {
                        "label": {"name": "Urgent"},
                        "card": {"id": card_iso_a, "shortLink": "StaleB22"},
                    },
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                },
            }
            shared_k_hdr = {
                "X-Auth-Token": "secret-webhook-token",
                "Content-Type": "application/json",
                "X-Delivery-Id": "shared-k",
            }
            iso_b_spawns = []
            iso_timers = []

            async def fake_iso_exec(*cmd, **kwargs):
                if card_iso_a in cmd[-1] or "RealA111" in cmd[-1]:
                    proc = FakeStreamProcess(lines=[json.dumps({"event": "init", "conversation_id": "conv-iso-a"})], hang_after_lines=True)
                    t_iso = threading.Timer(0.05, server.cleanup_processes)
                    iso_timers.append(t_iso)
                    t_iso.start()
                    return proc
                iso_b_spawns.append(list(cmd))
                return FakeStreamProcess(
                    lines=[
                        json.dumps({"event": "init", "conversation_id": "conv-iso-b"}),
                        json.dumps({"event": "result", "result": {"conversation_id": "conv-iso-b", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
                    ],
                    returncode=0,
                )

            # Admission does not call Trello API for non-comment action, keeping stale StaleB22 in payload before worker runs.
            with mock.patch.object(server, "fetch_card_details_sync", side_effect=AssertionError("Unexpected admission fetch")):
                req_ia = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps(spoof_iso_a).encode("utf-8"),
                    headers=shared_k_hdr,
                )
                with urllib.request.urlopen(req_ia, timeout=5) as resp_ia:
                    self.assertEqual(resp_ia.status, 202)
            qk_ia, qn_ia, qp_ia = server.task_queue.get_nowait()
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=live_iso_a),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_iso_exec),
            ):
                try:
                    server.run_agent_job(qk_ia, qn_ia, qp_ia)
                finally:
                    for t_iso in iso_timers:
                        t_iso.cancel()
                        t_iso.join(timeout=2.0)
                        self.assertFalse(t_iso.is_alive())
            server.shutdown_event.clear()
            server.outbound_posts_closed_event.clear()
            server.pending_cards.clear()
            self.assertTrue(server.requeue_in_flight_cards())

            st_iso = server.load_sidecar_state()
            tomb_ia = next(t for t in st_iso["unreconciled_tombstones"].values() if t.get("card_id") == card_iso_a)
            self.assertEqual(tomb_ia.get("short_card_id"), "RealA111")
            self.assertIn(f"{card_iso_a}:shared-k", tomb_ia.get("delivery_keys", []))
            self.assertIn("RealA111:shared-k", tomb_ia.get("delivery_keys", []))
            self.assertNotIn("StaleB22:shared-k", tomb_ia.get("delivery_keys", []))

            # Replay of Card A (canonical or verified short link) is blocked; Card B with same shared-k is accepted and runs.
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_iso_a):
                for replay_body in (
                    {"cardId": card_iso_a, "action": {"type": "addLabelToCard", "data": {"label": {"name": "Urgent"}}}},
                    {"cardLink": "https://trello.com/c/RealA111", "action": {"type": "addLabelToCard", "data": {"label": {"name": "Urgent"}}}},
                ):
                    req_ra = urllib.request.Request(
                        f"http://127.0.0.1:{port}/",
                        method="POST",
                        data=json.dumps(replay_body).encode("utf-8"),
                        headers=shared_k_hdr,
                    )
                    with urllib.request.urlopen(req_ra, timeout=5) as resp_ra:
                        self.assertEqual(json.loads(resp_ra.read().decode("utf-8")), {"status": "ignored", "reason": "unreconciled_action"})

            card_b_req = {
                "cardLink": "https://trello.com/c/StaleB22",
                "action": {
                    "type": "addLabelToCard",
                    "data": {"label": {"name": "Urgent"}},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                },
            }
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_iso_b):
                req_ib = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps(card_b_req).encode("utf-8"),
                    headers=shared_k_hdr,
                )
                with urllib.request.urlopen(req_ib, timeout=5) as resp_ib:
                    self.assertEqual(resp_ib.status, 202)
            qk_ib, qn_ib, qp_ib = server.task_queue.get_nowait()
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=live_iso_b),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_iso_exec),
            ):
                server.run_agent_job(qk_ib, qn_ib, qp_ib)
            self.assertEqual(len(iso_b_spawns), 1)
        finally:
            httpd.shutdown()
            httpd.server_close()

    def test_recovery_case_collision_and_admission_checkpoint(self):
        server = load_server()
        lower_key = "69bbcc112233445566778899"
        mixed_key = "69BBCC112233445566778899"

        # Verify both dict insertion orders ([mixed, lower] and [lower, mixed]) behave identically.
        for order in ([mixed_key, lower_key], [lower_key, mixed_key]):
            Path(server.SIDECAR_STATE_FILE).write_text("{}")
            server.pending_cards.clear()
            while not server.task_queue.empty():
                server.task_queue.get_nowait()

            in_flight = {}
            for key_name in order:
                act_id = "act-low-keep" if key_name == lower_key else "act-mix-drop"
                adm_id = "adm-low" if key_name == lower_key else "adm-mix"
                in_flight[key_name] = {
                    "card_name": "Case Card",
                    "payload": {"cardId": key_name, "action": {"id": act_id}, "comment": f"Comment {act_id}"},
                    "queued_at": 100.0 if key_name == lower_key else 200.0,
                    "stage": "queued",
                    "admission_id": adm_id,
                    "action_id": act_id,
                }
            Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({"in_flight_cards": in_flight}))
            self.assertTrue(server.requeue_in_flight_cards())

            q_key, _, q_payload = server.task_queue.get_nowait()
            self.assertTrue(server.task_queue.empty())
            self.assertEqual(q_key, lower_key)
            self.assertEqual(q_payload["action"]["id"], "act-low-keep")
            self.assertEqual(q_payload["_admission_id"], "adm-low")

            # Wrong admission_id is refused; matching admission_id dispatches and blocks second dispatch.
            self.assertFalse(server.mark_in_flight_dispatched(lower_key, admission_id="adm-mix"))
            self.assertTrue(server.mark_in_flight_dispatched(lower_key, admission_id=q_payload["_admission_id"], action_id="act-low-keep"))
            self.assertFalse(server.mark_in_flight_dispatched(lower_key, admission_id=q_payload["_admission_id"], action_id="act-low-keep"))

            # Next restart quarantines act-low-keep alongside act-mix-drop without losing either action ID.
            server.pending_cards.clear()
            self.assertTrue(server.requeue_in_flight_cards())
            st = server.load_sidecar_state()
            self.assertEqual(st.get("in_flight_cards"), {})
            u_actions = {v.get("action_id") for v in st.get("unreconciled_cards", {}).values()}
            self.assertEqual(u_actions, {"act-low-keep", "act-mix-drop"})

        # Queued/dispatched pair sharing the same action_id quarantines both without re-enqueuing.
        Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({
            "in_flight_cards": {
                mixed_key: {
                    "card_name": "Pair Card",
                    "payload": {"cardId": mixed_key, "action": {"id": "act-pair-1"}},
                    "queued_at": 100.0,
                    "stage": "queued",
                    "admission_id": "adm-pair-q",
                    "action_id": "act-pair-1",
                },
                lower_key: {
                    "card_name": "Pair Card",
                    "payload": {"cardId": lower_key, "action": {"id": "act-pair-1"}},
                    "queued_at": 90.0,
                    "stage": "dispatched",
                    "admission_id": "adm-pair-d",
                    "action_id": "act-pair-1",
                },
            }
        }))
        server.pending_cards.clear()
        self.assertTrue(server.requeue_in_flight_cards())
        self.assertTrue(server.task_queue.empty())

        # Two queued case-alias copies of the same event coalesce cleanly: 1 responder launch and 0 tombstones.
        for dup_order in ([mixed_key, lower_key], [lower_key, mixed_key]):
            Path(server.SIDECAR_STATE_FILE).write_text("{}")
            server.pending_cards.clear()
            while not server.task_queue.empty():
                server.task_queue.get_nowait()
            dup_in_flight = {}
            for key_name in dup_order:
                dup_in_flight[key_name] = {
                    "card_name": "Duplicate Queued Card",
                    "payload": {
                        "cardId": key_name,
                        "comment": "Same queued comment",
                        "action": {"id": "act-same-queued", "type": "commentCard", "memberCreator": {"username": "pm_user"}},
                    },
                    "queued_at": 100.0 if key_name == lower_key else 200.0,
                    "stage": "queued",
                    "admission_id": "adm-dup-low" if key_name == lower_key else "adm-dup-mix",
                    "action_id": "act-same-queued",
                }
            Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({"in_flight_cards": dup_in_flight}))
            self.assertTrue(server.requeue_in_flight_cards())
            qk_d, qn_d, qp_d = server.task_queue.get_nowait()
            self.assertTrue(server.task_queue.empty())
            dup_spawns = []

            async def fake_dup_exec(*cmd, **kwargs):
                dup_spawns.append(list(cmd))
                return FakeStreamProcess(
                    lines=[
                        json.dumps({"event": "init", "conversation_id": "conv-dup-ok"}),
                        json.dumps({"event": "result", "result": {"conversation_id": "conv-dup-ok", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
                    ],
                    returncode=0,
                )

            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_dup_exec),
            ):
                server.run_agent_job(qk_d, qn_d, qp_d)
            self.assertEqual(len(dup_spawns), 1)
            st_dup_done = server.load_sidecar_state()
            self.assertEqual(st_dup_done.get("in_flight_cards"), {})
            self.assertEqual(st_dup_done.get("unreconciled_tombstones", {}), {})
            self.assertEqual(st_dup_done.get("unreconciled_cards", {}), {})

        # Coalescing same-admission queued copies preserves ack_reserved on the non-canonical alias for ID-less triggers.
        ack_coal_server = load_server({"TRELLO_POST_ACK_COMMENTS": "true", "TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"})
        ack_coal_calls = []
        ack_coal_spawns = []
        coal_payload = {
            "cardId": lower_key,
            "comment": "Investigate coalesced ack",
            "triggerUserUsername": "pm_user",
        }
        self.assertTrue(ack_coal_server.should_accept_trigger(lower_key, "Coal Ack Card", coal_payload, "Investigate coalesced ack", "Ideas")[0])

        def coal_post_ack(cid, txt, kind="comment"):
            ack_coal_calls.append((cid, txt, kind))
            return True

        async def fake_coal_exec(*cmd, **kwargs):
            ack_coal_spawns.append(list(cmd))
            return FakeStreamProcess(
                lines=[
                    json.dumps({"event": "init", "conversation_id": "conv-coal-ok"}),
                    json.dumps({"event": "result", "result": {"conversation_id": "conv-coal-ok", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
                ],
                returncode=0,
            )

        with (
            mock.patch.object(ack_coal_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(ack_coal_server, "classify_action", side_effect=mock.AsyncMock(return_value="INVESTIGATOR")),
            mock.patch.object(ack_coal_server, "post_trello_comment", side_effect=coal_post_ack),
            mock.patch.object(ack_coal_server, "begin_process_launch", side_effect=lambda: (ack_coal_server.initiate_shutdown(), False)[1]),
        ):
            with self.assertRaises(ack_coal_server.ShutdownRequested):
                asyncio.run(ack_coal_server.trigger_agent(lower_key, "Coal Ack Card", copy.deepcopy(coal_payload)))
        self.assertEqual(len(ack_coal_calls), 1)

        st_coal = ack_coal_server.load_sidecar_state()
        reserved_row = st_coal["in_flight_cards"].pop(lower_key)
        self.assertTrue(reserved_row.get("ack_reserved"))
        unreserved_canon_copy = copy.deepcopy(reserved_row)
        unreserved_canon_copy["ack_reserved"] = False
        unreserved_canon_copy["queued_at"] = float(reserved_row.get("queued_at") or 0) - 5.0
        st_coal["in_flight_cards"][mixed_key] = reserved_row
        st_coal["in_flight_cards"][lower_key] = unreserved_canon_copy
        st_coal["last_ack_by_card"][lower_key] = time.time() - (ack_coal_server.ACK_COOLDOWN_SECONDS + 60)
        self.assertTrue(ack_coal_server.save_sidecar_state(st_coal))

        ack_coal_server.shutdown_event.clear()
        ack_coal_server.pending_cards.clear()
        while not ack_coal_server.task_queue.empty():
            ack_coal_server.task_queue.get_nowait()
        self.assertTrue(ack_coal_server.requeue_in_flight_cards())
        self.assertTrue(ack_coal_server.load_sidecar_state()["in_flight_cards"][lower_key].get("ack_reserved"))
        qk_ac, qn_ac, qp_ac = ack_coal_server.task_queue.get_nowait()
        self.assertTrue(ack_coal_server.task_queue.empty())
        with (
            mock.patch.object(ack_coal_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(ack_coal_server, "classify_action", side_effect=mock.AsyncMock(return_value="INVESTIGATOR")),
            mock.patch.object(ack_coal_server, "post_trello_comment", side_effect=coal_post_ack),
            mock.patch.object(ack_coal_server.asyncio, "create_subprocess_exec", side_effect=fake_coal_exec),
        ):
            ack_coal_server.run_agent_job(qk_ac, qn_ac, qp_ac)
        self.assertEqual(len(ack_coal_spawns), 1)
        self.assertEqual(len(ack_coal_calls), 1)

        # Coalescing same-admission queued copies syncs retained actionId + provenance upgrade into payload/queue and runs cleanly even if live lookup differs.
        coal_id_server = load_server({"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"})
        coal_canon = "69aa88000000000000000011"
        coal_mixed = "69AA88000000000000000011"
        coal_live_other = {
            "name": "Coal ID Card",
            "actions": [
                {
                    "id": "act-live-y",
                    "type": "commentCard",
                    "data": {"text": "Coalesce action"},
                    "memberCreator": {"username": "pm_user"},
                }
            ],
        }
        st_ci = {
            "in_flight_cards": {
                coal_canon: {
                    "card_name": "Coal ID Card",
                    "stage": "queued",
                    "admission_id": "adm-coal-act",
                    "admitted_without_action_id": True,
                    "action_id": None,
                    "action_id_inferred": False,
                    "queued_at": 200.0,
                    "payload": {
                        "cardId": coal_canon,
                        "cardName": "Coal ID Card",
                        "comment": "Coalesce action",
                        "triggerUserUsername": "pm_user",
                    },
                },
                coal_mixed: {
                    "card_name": "Coal ID Card",
                    "stage": "queued",
                    "admission_id": "adm-coal-act",
                    "admitted_without_action_id": False,
                    "action_id": "act-retained-x",
                    "action_id_inferred": False,
                    "queued_at": 100.0,
                    "payload": {
                        "cardId": coal_mixed,
                        "cardName": "Coal ID Card",
                        "actionId": "act-retained-x",
                        "comment": "Coalesce action",
                        "triggerUserUsername": "pm_user",
                    },
                },
            },
        }
        self.assertTrue(coal_id_server.save_sidecar_state(st_ci))
        coal_id_spawns = []

        async def fake_coal_id_exec(*cmd, **kwargs):
            coal_id_spawns.append(list(cmd))
            return FakeStreamProcess(
                lines=[
                    json.dumps({"event": "init", "conversation_id": "conv-coal-id"}),
                    json.dumps({"event": "result", "result": {"conversation_id": "conv-coal-id", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
                ],
                returncode=0,
            )

        self.assertTrue(coal_id_server.requeue_in_flight_cards())
        st_ci_after = coal_id_server.load_sidecar_state()
        self.assertEqual(list(st_ci_after["in_flight_cards"].keys()), [coal_canon])
        retained_ci = st_ci_after["in_flight_cards"][coal_canon]
        self.assertEqual(retained_ci.get("action_id"), "act-retained-x")
        self.assertFalse(retained_ci.get("action_id_inferred"))
        self.assertEqual(retained_ci["payload"].get("actionId"), "act-retained-x")
        qk_ci, qn_ci, qp_ci = coal_id_server.task_queue.get_nowait()
        self.assertTrue(coal_id_server.task_queue.empty())
        self.assertEqual(qp_ci.get("actionId"), "act-retained-x")
        self.assertFalse(qp_ci.get("_action_id_inferred"))
        with (
            mock.patch.object(coal_id_server, "fetch_card_details_sync", return_value=coal_live_other),
            mock.patch.object(coal_id_server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(coal_id_server.asyncio, "create_subprocess_exec", side_effect=fake_coal_id_exec),
        ):
            coal_id_server.run_agent_job(qk_ci, qn_ci, qp_ci)
        self.assertEqual(len(coal_id_spawns), 1)
        st_ci_done = coal_id_server.load_sidecar_state()
        self.assertEqual(st_ci_done.get("in_flight_cards"), {})
        self.assertEqual(st_ci_done.get("unreconciled_cards"), {})
        self.assertEqual(st_ci_done.get("unreconciled_tombstones", {}), {})

        # Coalescing upgrades inferred action_id/provenance to authoritative action_id across copies and payloads.
        coal_id_server.pending_cards.clear()
        st_prov = {
            "in_flight_cards": {
                coal_canon: {
                    "card_name": "Coal Prov Card",
                    "stage": "queued",
                    "admission_id": "adm-coal-prov",
                    "action_id": "act-inferred-y",
                    "action_id_inferred": True,
                    "queued_at": 200.0,
                    "payload": {
                        "cardId": coal_canon,
                        "cardName": "Coal Prov Card",
                        "actionId": "act-inferred-y",
                        "comment": "Coalesce prov",
                        "triggerUserUsername": "pm_user",
                    },
                },
                coal_mixed: {
                    "card_name": "Coal Prov Card",
                    "stage": "queued",
                    "admission_id": "adm-coal-prov",
                    "action_id": "act-auth-x",
                    "action_id_inferred": False,
                    "queued_at": 100.0,
                    "payload": {
                        "cardId": coal_mixed,
                        "cardName": "Coal Prov Card",
                        "actionId": "act-auth-x",
                        "comment": "Coalesce prov",
                        "triggerUserUsername": "pm_user",
                    },
                },
            },
        }
        self.assertTrue(coal_id_server.save_sidecar_state(st_prov))
        self.assertTrue(coal_id_server.requeue_in_flight_cards())
        st_prov_after = coal_id_server.load_sidecar_state()
        self.assertEqual(st_prov_after["in_flight_cards"][coal_canon].get("action_id"), "act-auth-x")
        self.assertFalse(st_prov_after["in_flight_cards"][coal_canon].get("action_id_inferred"))
        self.assertEqual(st_prov_after["in_flight_cards"][coal_canon]["payload"].get("actionId"), "act-auth-x")
        _, _, qp_prov = coal_id_server.task_queue.get_nowait()
        self.assertEqual(qp_prov.get("actionId"), "act-auth-x")
        self.assertFalse(qp_prov.get("_action_id_inferred"))

        # mark_in_flight_dispatched and trigger_agent atomically quarantine same-admission queued action_id conflicts
        # as stage="identity_conflict", and fall back to transient_error if quarantine persistence fails.
        conflict_server = load_server({"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"})
        c_conf_direct = "69aa88000000000000000021"
        c_conf_e2e = "69aa88000000000000000022"
        c_conf_fail = "69aa88000000000000000023"
        seed_queued_card(
            conflict_server,
            c_conf_direct,
            "Conf Direct",
            payload={"cardId": c_conf_direct, "comment": "Direct conflict", "triggerUserUsername": "pm_user"},
            admission_id="adm-conf-direct",
            action_id="act-queued-direct-x",
        )
        direct_detail = {}
        self.assertFalse(
            conflict_server.mark_in_flight_dispatched(
                c_conf_direct,
                admission_id="adm-conf-direct",
                action_id="act-conflict-direct-y",
                action_id_inferred=True,
                detail=direct_detail,
            )
        )
        self.assertEqual(direct_detail.get("status"), "identity_conflict")
        st_cd = conflict_server.load_sidecar_state()
        self.assertNotIn(c_conf_direct, st_cd.get("in_flight_cards", {}))
        self.assertTrue(
            any(
                r.get("stage") == "identity_conflict" and r.get("action_id") == "act-queued-direct-x"
                for r in st_cd.get("unreconciled_cards", {}).values()
            )
        )
        self.assertTrue(
            any(
                t.get("action_id") == "act-queued-direct-x" and t.get("card_id") == c_conf_direct
                for t in st_cd.get("unreconciled_tombstones", {}).values()
            )
        )

        e2e_payload = {"cardId": c_conf_e2e, "comment": "Trigger e2e conflict", "triggerUserUsername": "pm_user"}
        seed_queued_card(
            conflict_server,
            c_conf_e2e,
            "Conf E2E",
            payload=dict(e2e_payload),
            admission_id="adm-conf-e2e",
            action_id="act-queued-e2e-x",
        )
        e2e_payload["_admission_id"] = "adm-conf-e2e"
        conflict_server.pending_cards.add(c_conf_e2e)
        live_conf_e2e = {
            "name": "Conf E2E",
            "actions": [
                {
                    "id": "act-live-conflict-y",
                    "type": "commentCard",
                    "data": {"text": "Trigger e2e conflict"},
                    "memberCreator": {"username": "pm_user"},
                }
            ],
        }
        with (
            mock.patch.object(conflict_server, "fetch_card_details_sync", return_value=live_conf_e2e),
            mock.patch.object(conflict_server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(conflict_server.asyncio, "create_subprocess_exec") as mock_conf_exec,
        ):
            conflict_server.run_agent_job(c_conf_e2e, "Conf E2E", dict(e2e_payload))
        mock_conf_exec.assert_not_called()
        self.assertNotIn(c_conf_e2e, conflict_server.pending_cards)
        st_ce = conflict_server.load_sidecar_state()
        self.assertNotIn(c_conf_e2e, st_ce.get("in_flight_cards", {}))
        self.assertTrue(
            any(
                r.get("stage") == "identity_conflict" and r.get("action_id") == "act-queued-e2e-x"
                for r in st_ce.get("unreconciled_cards", {}).values()
            )
        )
        conf_events = [json.loads(l) for l in Path(conflict_server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertTrue(any(ev.get("event") == "trigger_ignored" and ev.get("reason") == "identity_conflict" for ev in conf_events))
        self.assertFalse(any(ev.get("event") == "agent_run_error" for ev in conf_events))
        self.assertEqual(
            conflict_server.should_accept_trigger(
                c_conf_e2e,
                "Conf E2E",
                {"cardId": c_conf_e2e, "action": {"id": "act-queued-e2e-x", "type": "commentCard"}, "comment": "Trigger e2e conflict"},
                "Trigger e2e conflict",
                "",
            ),
            (False, "unreconciled_action"),
        )

        seed_queued_card(
            conflict_server,
            c_conf_fail,
            "Conf Save Fail",
            payload={"cardId": c_conf_fail, "comment": "Save fail conflict"},
            admission_id="adm-conf-fail",
            action_id="act-queued-fail-x",
        )
        fail_detail = {}
        with mock.patch.object(conflict_server, "save_sidecar_state", return_value=False):
            self.assertFalse(
                conflict_server.mark_in_flight_dispatched(
                    c_conf_fail,
                    admission_id="adm-conf-fail",
                    action_id="act-conflict-fail-y",
                    action_id_inferred=True,
                    detail=fail_detail,
                )
            )
        self.assertEqual(fail_detail.get("status"), "transient_error")
        st_cf = conflict_server.load_sidecar_state()
        self.assertEqual(st_cf["in_flight_cards"][c_conf_fail].get("stage"), "queued")
        self.assertEqual(st_cf["in_flight_cards"][c_conf_fail].get("action_id"), "act-queued-fail-x")
        self.assertFalse(any(r.get("card_id") == c_conf_fail for r in st_cf.get("unreconciled_cards", {}).values()))

        # Startup queued recovery, ack reservation, and dispatch checkpoint screen queued replays against full quarantine
        # (including legacy short-key fingerprint tombstones matched via verified card_aliases).
        fq_server = load_server({"TRELLO_POST_ACK_COMMENTS": "true", "TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"})
        fq_canon = "69ee11000000000000000001"
        fq_other = "69ee22000000000000000002"
        fq_short = "Sh0rtQ3"
        fq_comment = "Same legacy queued"
        fq_fp_short = fq_server.trigger_fingerprint(fq_short, {"cardId": fq_short, "comment": fq_comment}, fq_comment, "")
        Path(fq_server.SIDECAR_STATE_FILE).write_text(json.dumps({
            "card_aliases": {fq_short: fq_canon},
            "unreconciled_fingerprints": {
                fq_fp_short: {"card_id": fq_short, "interrupted_at": 100.0},
            },
            "in_flight_cards": {
                fq_canon: {
                    "card_name": "Quarantined Queued",
                    "payload": {
                        "cardId": fq_canon,
                        "cardLink": f"https://trello.com/c/{fq_short}",
                        "comment": fq_comment,
                        "triggerUserUsername": "pm_user",
                    },
                    "queued_at": 110.0,
                    "stage": "queued",
                    "admission_id": "adm-fq-drop",
                    "admitted_without_action_id": True,
                },
                fq_other: {
                    "card_name": "Healthy Queued",
                    "payload": {"cardId": fq_other, "comment": "Distinct queued", "triggerUserUsername": "pm_user"},
                    "queued_at": 120.0,
                    "stage": "queued",
                    "admission_id": "adm-fq-keep",
                    "action_id": "act-fq-keep",
                },
            },
        }))
        fq_server.pending_cards.clear()
        self.assertTrue(fq_server.requeue_in_flight_cards())
        requeued = []
        while not fq_server.task_queue.empty():
            requeued.append(fq_server.task_queue.get_nowait())
        self.assertEqual([item[0] for item in requeued], [fq_other])

        fq_spawns = []

        async def fake_fq_ok(*cmd, **kwargs):
            fq_spawns.append(list(cmd))
            return FakeStreamProcess(
                lines=[
                    json.dumps({"event": "init", "conversation_id": "conv-fq-other"}),
                    json.dumps({"event": "result", "result": {"conversation_id": "conv-fq-other", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
                ],
                returncode=0,
            )

        with (
            mock.patch.object(fq_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(fq_server, "classify_action", side_effect=mock.AsyncMock(return_value="INVESTIGATOR")),
            mock.patch.object(fq_server, "post_trello_comment", return_value=True) as mock_fq_ack,
            mock.patch.object(fq_server.asyncio, "create_subprocess_exec", side_effect=fake_fq_ok),
        ):
            for qk, qn, qp in requeued:
                fq_server.run_agent_job(qk, qn, qp)
        self.assertEqual(mock_fq_ack.call_count, 1)
        self.assertEqual(mock_fq_ack.call_args.args[0], fq_other)
        self.assertEqual(len(fq_spawns), 1)

        # Direct ack reservation and dispatch checkpoint checks also block a queued canonical replay against legacy fingerprint tombstones.
        Path(fq_server.SIDECAR_STATE_FILE).write_text(json.dumps({
            "card_aliases": {fq_short: fq_canon},
            "unreconciled_tombstones": {
                "legacy-fp-q": {
                    "record_id": "legacy-fp-q",
                    "card_id": fq_short,
                    "action_id": None,
                    "fingerprints": [],
                    "legacy_fingerprints": [fq_fp_short],
                    "legacy_headerless": True,
                    "interrupted_at": 100.0,
                }
            },
            "in_flight_cards": {
                fq_canon: {
                    "card_name": "Quarantined Queued",
                    "payload": {
                        "cardId": fq_canon,
                        "cardLink": f"https://trello.com/c/{fq_short}",
                        "comment": fq_comment,
                        "triggerUserUsername": "pm_user",
                    },
                    "queued_at": 110.0,
                    "stage": "queued",
                    "admission_id": "adm-chk-q",
                    "admitted_without_action_id": True,
                }
            },
        }))
        self.assertFalse(fq_server.reserve_ack_comment(fq_canon, "INVESTIGATOR", admission_id="adm-chk-q"))
        cp_q_detail = {}
        self.assertFalse(fq_server.mark_in_flight_dispatched(fq_canon, admission_id="adm-chk-q", detail=cp_q_detail))
        self.assertEqual(cp_q_detail.get("status"), "quarantined")

    def test_shutdown_launch_race_and_outbound_comment_boundaries(self):
        # (a) Child spawn overlapping cleanup_processes() is awaited, registered, and terminated.
        server = load_server()
        card_key = "69ddd2222222222222222222"
        payload = {"cardId": card_key, "comment": "Race test"}
        seed_queued_card(server, card_key, "Race Card", payload=payload, admission_id="adm-race-1")
        payload["_admission_id"] = "adm-race-1"

        async def run_race():
            entered_spawn = asyncio.Event()
            release_spawn = asyncio.Event()
            spawned = []

            async def suspended_exec(*cmd, **kwargs):
                entered_spawn.set()
                await release_spawn.wait()
                proc = FakeStreamProcess(hang_after_lines=True)
                spawned.append(proc)
                return proc

            cleanup_done = threading.Event()

            def run_cleanup():
                server.cleanup_processes()
                cleanup_done.set()

            async def _driver():
                task = None
                cleaner = None
                try:
                    task = asyncio.create_task(server.trigger_agent(card_key, "Race Card", dict(payload)))
                    await entered_spawn.wait()
                    cleaner = threading.Thread(target=run_cleanup, daemon=True)
                    cleaner.start()
                    await asyncio.sleep(0.05)
                    self.assertFalse(cleanup_done.is_set())
                    release_spawn.set()
                    with self.assertRaises(server.ShutdownRequested):
                        await task
                    cleaner.join(timeout=2.0)
                    self.assertTrue(cleanup_done.is_set())
                    self.assertEqual(len(spawned), 1)
                    self.assertTrue(spawned[0].terminated or spawned[0].killed)
                finally:
                    release_spawn.set()
                    if task is not None and not task.done():
                        task.cancel()
                        try:
                            await task
                        except (asyncio.CancelledError, server.ShutdownRequested):
                            pass
                    if cleaner is not None:
                        cleaner.join(timeout=2.0)
                        self.assertFalse(cleaner.is_alive())

            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=suspended_exec),
            ):
                await _driver()

        asyncio.run(run_race())

        # (b) Shutdown triggered right before outbound acknowledgement/failure comment prevents Trello writes.
        ack_server = load_server({"TRELLO_POST_ACK_COMMENTS": "true"})
        ack_card = "69ddd3333333333333333333"
        ack_payload = {"cardId": ack_card, "comment": "Investigate", "action": {"id": "act-ack-sd", "type": "commentCard"}}
        seed_queued_card(ack_server, ack_card, "Ack SD", payload=ack_payload, admission_id="adm-ack-sd", action_id="act-ack-sd")
        ack_payload["_admission_id"] = "adm-ack-sd"
        orig_reserve = ack_server.reserve_ack_comment

        def reserve_then_shutdown(*args, **kwargs):
            ok = orig_reserve(*args, **kwargs)
            ack_server.initiate_shutdown()
            return ok

        with (
            mock.patch.object(ack_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(ack_server, "classify_action", side_effect=mock.AsyncMock(return_value="INVESTIGATOR")),
            mock.patch.object(ack_server, "reserve_ack_comment", side_effect=reserve_then_shutdown),
            mock.patch.object(ack_server, "post_trello_comment") as mock_ack_post,
            mock.patch.object(ack_server.asyncio, "create_subprocess_exec") as mock_ack_exec,
        ):
            with self.assertRaises(ack_server.ShutdownRequested):
                asyncio.run(ack_server.trigger_agent(ack_card, "Ack SD", dict(ack_payload)))
        mock_ack_post.assert_not_called()
        mock_ack_exec.assert_not_called()

        fail_server = load_server()
        fail_card = "69ddd4444444444444444444"
        fail_payload = {"cardId": fail_card, "comment": "Fail"}
        seed_queued_card(fail_server, fail_card, "Fail SD", payload=fail_payload, admission_id="adm-fail-sd")
        fail_payload["_admission_id"] = "adm-fail-sd"
        orig_log_event = fail_server.log_sidecar_event

        def log_then_shutdown(event_name, details):
            orig_log_event(event_name, details)
            if event_name == "agent_run_failed":
                fail_server.initiate_shutdown()

        async def fail_fast(*cmd, **kwargs):
            return FakeStreamProcess(lines=[json.dumps({"event": "result", "result": {"status": "ERROR", "finished_reason": "print_timeout"}})], returncode=3)

        with (
            mock.patch.object(fail_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(fail_server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(fail_server, "log_sidecar_event", side_effect=log_then_shutdown),
            mock.patch.object(fail_server, "post_trello_comment") as mock_fail_post,
            mock.patch.object(fail_server.asyncio, "create_subprocess_exec", side_effect=fail_fast),
        ):
            with self.assertRaises(fail_server.ShutdownRequested):
                asyncio.run(fail_server.trigger_agent(fail_card, "Fail SD", dict(fail_payload)))
        mock_fail_post.assert_not_called()

        # (c) Shutdown during dispatch save_sidecar_state rolls back dispatched -> queued and spawns 0 processes.
        cp_server = load_server()
        cp_card = "69ddd5555555555555555555"
        cp_payload = {"cardId": cp_card, "comment": "Checkpoint race"}
        seed_queued_card(cp_server, cp_card, "CP Card", payload=cp_payload, admission_id="adm-cp-1")
        cp_payload["_admission_id"] = "adm-cp-1"
        orig_cp_save = cp_server.save_sidecar_state

        def save_then_shutdown(state):
            in_fl = state.get("in_flight_cards", {}).get(cp_card, {})
            ok = orig_cp_save(state)
            if in_fl.get("stage") == "dispatched":
                cp_server.initiate_shutdown()
            return ok

        with (
            mock.patch.object(cp_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(cp_server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(cp_server, "save_sidecar_state", side_effect=save_then_shutdown),
            mock.patch.object(cp_server.asyncio, "create_subprocess_exec") as mock_cp_exec,
        ):
            cp_server.run_agent_job(cp_card, "CP Card", dict(cp_payload))
        mock_cp_exec.assert_not_called()
        self.assertEqual(cp_server.load_sidecar_state()["in_flight_cards"][cp_card]["stage"], "queued")

        # (d) Shutdown firing after executor submission but before post_trello_comment urlopen skips Trello HTTP call.
        url_server = load_server({"TRELLO_POST_ACK_COMMENTS": "true", "TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"})
        url_card = "69ddd6666666666666666666"
        url_payload = {"cardId": url_card, "comment": "Investigate", "action": {"id": "act-url-sd", "type": "commentCard"}}
        seed_queued_card(url_server, url_card, "URL SD", payload=url_payload, admission_id="adm-url-sd", action_id="act-url-sd")
        url_payload["_admission_id"] = "adm-url-sd"
        orig_post_comment = url_server.post_trello_comment

        def shutdown_inside_post(cid, txt, kind="comment"):
            url_server.initiate_shutdown()
            return orig_post_comment(cid, txt, kind=kind)

        with (
            mock.patch.object(url_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(url_server, "classify_action", side_effect=mock.AsyncMock(return_value="INVESTIGATOR")),
            mock.patch.object(url_server, "post_trello_comment", side_effect=shutdown_inside_post),
            mock.patch.object(url_server.urllib.request, "urlopen") as mock_urlopen,
            mock.patch.object(url_server.asyncio, "create_subprocess_exec") as mock_url_exec,
        ):
            with self.assertRaises(url_server.ShutdownRequested):
                asyncio.run(url_server.trigger_agent(url_card, "URL SD", dict(url_payload)))
        mock_urlopen.assert_not_called()
        mock_url_exec.assert_not_called()

        # (e) Shutdown between mark_in_flight_dispatched and create_subprocess_exec with failed first rollback retry:
        # 0 spawns, 0 agent_run_started events, queued row recovered cleanly after restart.
        pre_server = load_server()
        pre_card = "69ddd7777777777777777777"
        pre_payload = {"cardId": pre_card, "comment": "Pre-spawn barrier"}
        seed_queued_card(pre_server, pre_card, "Pre-Spawn", payload=pre_payload, admission_id="adm-pre-sd")
        pre_payload["_admission_id"] = "adm-pre-sd"
        orig_mark_disp = pre_server.mark_in_flight_dispatched
        orig_pre_save = pre_server.save_sidecar_state
        rollback_saves = {"failed_once": False}

        def mark_then_shutdown(*args, **kwargs):
            ok = orig_mark_disp(*args, **kwargs)
            if ok:
                pre_server.initiate_shutdown()
            return ok

        def save_fail_first_rollback(state):
            in_fl = state.get("in_flight_cards", {}).get(pre_card, {})
            if in_fl.get("stage") == "queued" and not rollback_saves["failed_once"]:
                rollback_saves["failed_once"] = True
                return False
            return orig_pre_save(state)

        with (
            mock.patch.object(pre_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(pre_server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(pre_server, "mark_in_flight_dispatched", side_effect=mark_then_shutdown),
            mock.patch.object(pre_server, "save_sidecar_state", side_effect=save_fail_first_rollback),
            mock.patch.object(pre_server.asyncio, "create_subprocess_exec") as mock_pre_exec,
        ):
            pre_server.run_agent_job(pre_card, "Pre-Spawn", dict(pre_payload))
        mock_pre_exec.assert_not_called()
        self.assertTrue(rollback_saves["failed_once"])
        self.assertEqual(pre_server.load_sidecar_state()["in_flight_cards"][pre_card]["stage"], "queued")
        pre_events = [json.loads(l)["event"] for l in Path(pre_server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertNotIn("agent_run_started", pre_events)

        pre_server.shutdown_event.clear()
        pre_server.pending_cards.clear()
        while not pre_server.task_queue.empty():
            pre_server.task_queue.get_nowait()
        self.assertTrue(pre_server.requeue_in_flight_cards())
        self.assertEqual(pre_server.task_queue.get_nowait()[0], pre_card)

        # (f) Outbound post_trello_comment coordinates initiation with shutdown and blocks cleanup_processes while active.
        post_sd_server = load_server({"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"})
        orig_begin_post = post_sd_server.begin_outbound_post

        def begin_post_then_shutdown():
            ok = orig_begin_post()
            post_sd_server.initiate_shutdown()
            return ok

        with (
            mock.patch.dict(os.environ, {"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"}, clear=False),
            mock.patch.object(post_sd_server, "begin_outbound_post", side_effect=begin_post_then_shutdown),
            mock.patch.object(post_sd_server.urllib.request, "urlopen") as mock_url_gate,
        ):
            self.assertFalse(post_sd_server.post_trello_comment("69ddd8888888888888888888", "Skipped", "comment"))
        mock_url_gate.assert_not_called()

        # POST boundary barrier separating reserved POST from transport entry refuses urlopen once shutdown owns the boundary.
        boundary_server = load_server({"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"})
        entered_post_boundary = threading.Event()
        release_post_boundary = threading.Event()
        barrier_clean_done = threading.Event()
        orig_open_post = boundary_server._open_outbound_post_request
        boundary_result = {}

        def paused_open_post(req, timeout=10):
            entered_post_boundary.set()
            release_post_boundary.wait(timeout=2.0)
            return orig_open_post(req, timeout=timeout)

        with (
            mock.patch.dict(os.environ, {"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"}, clear=False),
            mock.patch.object(boundary_server, "_open_outbound_post_request", side_effect=paused_open_post),
            mock.patch.object(boundary_server.urllib.request, "urlopen") as mock_url_boundary,
        ):
            t_bpost = threading.Thread(
                target=lambda: boundary_result.update({"ok": boundary_server.post_trello_comment("69ddd8888888888888888889", "Boundary Blocked", "comment")}),
                daemon=True,
            )
            t_bclean = None
            try:
                t_bpost.start()
                self.assertTrue(entered_post_boundary.wait(timeout=1.5))
                with boundary_server.process_cond:
                    self.assertEqual(boundary_server.outbound_posts_in_progress, 1)
                t_bclean = threading.Thread(target=lambda: (boundary_server.cleanup_processes(), barrier_clean_done.set()), daemon=True)
                t_bclean.start()
                deadline_sd = time.monotonic() + 1.0
                while not boundary_server.shutdown_event.is_set() and time.monotonic() < deadline_sd:
                    time.sleep(0.01)
                self.assertTrue(boundary_server.shutdown_event.is_set())
                self.assertFalse(barrier_clean_done.is_set())
                release_post_boundary.set()
                t_bclean.join(timeout=2.0)
                t_bpost.join(timeout=2.0)
            finally:
                release_post_boundary.set()
                for th in (t_bclean, t_bpost):
                    if th is not None:
                        th.join(timeout=2.0)
                        self.assertFalse(th.is_alive())
        self.assertTrue(barrier_clean_done.is_set())
        self.assertFalse(boundary_result.get("ok"))
        mock_url_boundary.assert_not_called()

        # Inside-gate barrier after the outer gate check and before _invoke_outbound_urlopen enters transport:
        # cleanup_processes() sets shutdown_event and signals active responders immediately; _invoke_outbound_urlopen aborts urlopen.
        inside_server = load_server({"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"})
        entered_gate_inner = threading.Event()
        release_gate_inner = threading.Event()
        inside_clean_done = threading.Event()
        inside_c2_signaled = threading.Event()
        inside_c2_pgid = os.getpid() + 100008
        orig_invoke_urlopen = inside_server._invoke_outbound_urlopen
        inside_res = {}

        class InsideFakeGroupProc:
            def __init__(self, pid):
                self.pid = pid
                self.returncode = None

            def terminate(self):
                self.returncode = -signal.SIGTERM
                inside_c2_signaled.set()

            def kill(self):
                self.returncode = -signal.SIGKILL

        inside_c2_proc = InsideFakeGroupProc(inside_c2_pgid)
        with inside_server.process_cond:
            inside_server.active_processes[inside_c2_proc] = inside_c2_pgid

        def paused_invoke_urlopen(req, timeout=10):
            entered_gate_inner.set()
            release_gate_inner.wait(timeout=2.0)
            return orig_invoke_urlopen(req, timeout=timeout)

        def inside_fake_killpg(pid, sig):
            if pid == inside_c2_pgid and sig == signal.SIGTERM:
                inside_c2_proc.terminate()
                return True
            return False

        def inside_fake_pg_exists(pid):
            return pid == inside_c2_pgid and inside_c2_proc.returncode is None

        with (
            mock.patch.dict(os.environ, {"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"}, clear=False),
            mock.patch.object(inside_server, "_invoke_outbound_urlopen", side_effect=paused_invoke_urlopen),
            mock.patch.object(inside_server.urllib.request, "urlopen") as mock_inside_urlopen,
            mock.patch.object(inside_server, "_kill_process_group", side_effect=inside_fake_killpg),
            mock.patch.object(inside_server, "_process_group_exists", side_effect=inside_fake_pg_exists),
        ):
            t_in_post = threading.Thread(
                target=lambda: inside_res.update({"ok": inside_server.post_trello_comment("69ddd888888888888888888a", "Inside Gate", "comment")}),
                daemon=True,
            )
            t_in_clean = None
            try:
                t_in_post.start()
                self.assertTrue(entered_gate_inner.wait(timeout=1.5))
                t_in_clean = threading.Thread(target=lambda: (inside_server.cleanup_processes(), inside_clean_done.set()), daemon=True)
                t_in_clean.start()
                self.assertTrue(inside_c2_signaled.wait(timeout=0.3))
                time.sleep(0.02)
                self.assertFalse(inside_server.outbound_posts_closed_event.is_set())
                self.assertFalse(inside_clean_done.is_set())
                release_gate_inner.set()
                t_in_clean.join(timeout=2.0)
                t_in_post.join(timeout=2.0)
                mock_inside_urlopen.assert_not_called()
            finally:
                release_gate_inner.set()
                for th in (t_in_clean, t_in_post):
                    if th is not None:
                        th.join(timeout=2.0)
                        self.assertFalse(th.is_alive())
        self.assertTrue(inside_clean_done.is_set())
        self.assertTrue(inside_server.outbound_posts_closed_event.is_set())
        self.assertFalse(inside_res.get("ok"))

        # Timed-out _close_outbound_post_gate still closes outbound_posts_closed_event + shutdown_event and blocks later POSTs.
        timeout_server = load_server({"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"})
        entered_timeout_post = threading.Event()
        release_timeout_post = threading.Event()
        timeout_post_res = {}

        class TimeoutResp:
            status = 200
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False

        def stalled_timeout_urlopen(req, timeout=10):
            entered_timeout_post.set()
            release_timeout_post.wait(timeout=2.0)
            return TimeoutResp()

        with (
            mock.patch.dict(os.environ, {"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"}, clear=False),
            mock.patch.object(timeout_server.urllib.request, "urlopen", side_effect=stalled_timeout_urlopen) as mock_timeout_urlopen,
        ):
            t_timeout_post = threading.Thread(
                target=lambda: timeout_post_res.update({"ok": timeout_server.post_trello_comment("69ddd888888888888888888b", "Stalled Gate", "comment")}),
                daemon=True,
            )
            try:
                t_timeout_post.start()
                self.assertTrue(entered_timeout_post.wait(timeout=1.5))
                self.assertFalse(timeout_server._close_outbound_post_gate(timeout=0.05))
                self.assertTrue(timeout_server.outbound_posts_closed_event.is_set())
                self.assertTrue(timeout_server.shutdown_event.is_set())
                self.assertFalse(timeout_server.post_trello_comment("69ddd888888888888888888c", "Blocked After Timeout", "comment"))
                self.assertEqual(mock_timeout_urlopen.call_count, 1)
            finally:
                release_timeout_post.set()
                t_timeout_post.join(timeout=2.0)
                self.assertFalse(t_timeout_post.is_alive())
            self.assertIsNone(timeout_server._open_outbound_post_request(urllib.request.Request("https://example.invalid", method="POST")))
            self.assertEqual(mock_timeout_urlopen.call_count, 1)
        self.assertTrue(timeout_post_res.get("ok"))

        inflight_post_server = load_server({"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"})
        entered_urlopen = threading.Event()
        release_urlopen = threading.Event()
        cleanup_post_done = threading.Event()

        class FakeResp:
            status = 200
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False

        def slow_urlopen(req, timeout=10):
            entered_urlopen.set()
            release_urlopen.wait(timeout=2.0)
            return FakeResp()

        post_result = {}

        def do_post_thread():
            post_result["ok"] = inflight_post_server.post_trello_comment("69ddd9999999999999999999", "Wait", "comment")

        # Active process group on another card receives prompt SIGTERM even while Card 1's urlopen POST is stalled.
        card2_signaled = threading.Event()
        card2_pgid = os.getpid() + 100001

        class FakeGroupLeader:
            def __init__(self, pid):
                self.pid = pid
                self.returncode = None

            def terminate(self):
                self.returncode = -signal.SIGTERM
                card2_signaled.set()

            def kill(self):
                self.returncode = -signal.SIGKILL

        card2_proc = FakeGroupLeader(card2_pgid)
        with inflight_post_server.process_cond:
            inflight_post_server.active_processes[card2_proc] = card2_pgid

        def fake_killpg(pid, sig):
            if pid == card2_pgid and sig == signal.SIGTERM:
                card2_proc.terminate()
                return True
            return False

        def fake_pg_exists(pid):
            return pid == card2_pgid and card2_proc.returncode is None

        with (
            mock.patch.dict(os.environ, {"TRELLO_API_KEY": "k", "TRELLO_API_TOKEN": "t"}, clear=False),
            mock.patch.object(inflight_post_server.urllib.request, "urlopen", side_effect=slow_urlopen),
            mock.patch.object(inflight_post_server, "_kill_process_group", side_effect=fake_killpg),
            mock.patch.object(inflight_post_server, "_process_group_exists", side_effect=fake_pg_exists),
        ):
            t_post = threading.Thread(target=do_post_thread, daemon=True)
            t_clean = None
            try:
                t_post.start()
                self.assertTrue(entered_urlopen.wait(timeout=1.5))
                t_clean = threading.Thread(target=lambda: (inflight_post_server.cleanup_processes(), cleanup_post_done.set()), daemon=True)
                t_clean.start()
                self.assertTrue(card2_signaled.wait(timeout=0.3))
                self.assertFalse(cleanup_post_done.is_set())
                release_urlopen.set()
                t_clean.join(timeout=2.0)
                t_post.join(timeout=2.0)
            finally:
                release_urlopen.set()
                for th in (t_clean, t_post):
                    if th is not None:
                        th.join(timeout=2.0)
                        self.assertFalse(th.is_alive())
        self.assertTrue(cleanup_post_done.is_set())
        self.assertTrue(post_result.get("ok"))

        # (g) Barrier paused inside _spawn_gate_lock after begin_process_launch():
        # another card's active process group is signaled immediately, and the paused launch is registered and terminated.
        gate_server = load_server()
        gate_c1 = "69ddd7700000000000000001"
        gate_payload = {"cardId": gate_c1, "comment": "Gate barrier"}
        seed_queued_card(gate_server, gate_c1, "Gate C1", payload=gate_payload, admission_id="adm-gate-1")
        gate_payload["_admission_id"] = "adm-gate-1"

        entered_gate = threading.Event()
        release_gate = threading.Event()
        gate_clean_done = threading.Event()
        gate_c2_signaled = threading.Event()
        gate_c2_pgid = os.getpid() + 100002
        gate_c1_spawned = []
        gate_result = {}

        class FakeActiveProc:
            def __init__(self, pid):
                self.pid = pid
                self.returncode = None

            def terminate(self):
                self.returncode = -signal.SIGTERM
                if self.pid == gate_c2_pgid:
                    gate_c2_signaled.set()

            def kill(self):
                self.returncode = -signal.SIGKILL

        gate_c2_proc = FakeActiveProc(gate_c2_pgid)
        with gate_server.process_cond:
            gate_server.active_processes[gate_c2_proc] = gate_c2_pgid

        orig_begin_gate = gate_server.begin_process_launch

        def begin_then_pause():
            ok = orig_begin_gate()
            entered_gate.set()
            release_gate.wait(timeout=2.0)
            return ok

        async def gate_exec(*cmd, **kwargs):
            proc = FakeStreamProcess(hang_after_lines=True, pid=os.getpid() + 100003)
            gate_c1_spawned.append(proc)
            return proc

        def run_c1():
            try:
                asyncio.run(gate_server.trigger_agent(gate_c1, "Gate C1", dict(gate_payload)))
            except gate_server.ShutdownRequested:
                gate_result["shutdown"] = True

        def fake_gate_killpg(pid, sig):
            if pid == gate_c2_pgid and sig == signal.SIGTERM:
                gate_c2_proc.terminate()
                return True
            if gate_c1_spawned and pid == gate_c1_spawned[0].pid:
                if sig == signal.SIGTERM:
                    gate_c1_spawned[0].terminate()
                else:
                    gate_c1_spawned[0].kill()
                return True
            return False

        def fake_gate_pg_exists(pid):
            if pid == gate_c2_pgid:
                return gate_c2_proc.returncode is None
            if gate_c1_spawned and pid == gate_c1_spawned[0].pid:
                return gate_c1_spawned[0].returncode is None
            return False

        with (
            mock.patch.object(gate_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(gate_server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(gate_server, "begin_process_launch", side_effect=begin_then_pause),
            mock.patch.object(gate_server.asyncio, "create_subprocess_exec", side_effect=gate_exec),
            mock.patch.object(gate_server, "_kill_process_group", side_effect=fake_gate_killpg),
            mock.patch.object(gate_server, "_process_group_exists", side_effect=fake_gate_pg_exists),
        ):
            t_c1 = threading.Thread(target=run_c1, daemon=True)
            t_clean_g = None
            try:
                t_c1.start()
                self.assertTrue(entered_gate.wait(timeout=1.5))
                t_clean_g = threading.Thread(target=lambda: (gate_server.cleanup_processes(), gate_clean_done.set()), daemon=True)
                t_clean_g.start()
                self.assertTrue(gate_c2_signaled.wait(timeout=0.3))
                self.assertFalse(gate_clean_done.is_set())
                release_gate.set()
                t_clean_g.join(timeout=2.0)
                t_c1.join(timeout=2.0)
            finally:
                release_gate.set()
                for th in (t_clean_g, t_c1):
                    if th is not None:
                        th.join(timeout=2.0)
                        self.assertFalse(th.is_alive())
        self.assertTrue(gate_clean_done.is_set())
        self.assertTrue(gate_result.get("shutdown"))
        self.assertEqual(len(gate_c1_spawned), 1)
        self.assertTrue(gate_c1_spawned[0].terminated or gate_c1_spawned[0].killed)

        # (h) Actual service entry point run() signals active worker Card B AND sets shutdown_event BEFORE releasing Card A from _spawn_gate_lock:
        # Card B exits on SIGTERM via shutdown path with zero failure POSTs and keeps its dispatched row for startup quarantine.
        run_server = load_server()
        run_c1 = "69ddd7700000000000000099"
        run_c2 = "69ddd7700000000000000088"
        run_payload = {"cardId": run_c1, "comment": "Entry point barrier"}
        run_c2_payload = {"cardId": run_c2, "comment": "External write before shutdown", "triggerUserUsername": "pm_user"}
        seed_queued_card(run_server, run_c1, "Run C1", payload=run_payload, admission_id="adm-run-1")
        seed_queued_card(run_server, run_c2, "Run C2", payload=run_c2_payload, admission_id="adm-run-2")
        run_payload["_admission_id"] = "adm-run-1"
        run_c2_payload["_admission_id"] = "adm-run-2"

        run_entered_gate = threading.Event()
        run_release_gate = threading.Event()
        run_c2_started = threading.Event()
        run_c2_signaled = threading.Event()
        run_c2_pgid = os.getpid() + 100004
        run_c1_pgid = os.getpid() + 100005
        run_c1_spawned = []
        run_c2_spawned = []
        run_c1_res = {}

        orig_run_begin = run_server.begin_process_launch

        def run_begin_then_pause():
            ok = orig_run_begin()
            if run_c2_started.is_set() and not run_entered_gate.is_set():
                run_entered_gate.set()
                run_release_gate.wait(timeout=2.0)
            return ok

        async def run_gate_exec(*cmd, **kwargs):
            prompt_arg = cmd[-1] if cmd else ""
            if run_c2 in prompt_arg or "External write before shutdown" in prompt_arg:
                proc = FakeStreamProcess(
                    lines=[
                        json.dumps({"event": "init", "conversation_id": "conv-run-c2"}),
                        json.dumps({"event": "tool_use", "name": "bash"}),
                    ],
                    hang_after_lines=True,
                    pid=run_c2_pgid,
                )
                run_c2_spawned.append(proc)
                run_c2_started.set()
                return proc
            proc = FakeStreamProcess(hang_after_lines=True, pid=run_c1_pgid)
            run_c1_spawned.append(proc)
            return proc

        def run_c2_worker():
            run_server.run_agent_job(run_c2, "Run C2", copy.deepcopy(run_c2_payload))

        def run_c1_worker():
            try:
                asyncio.run(run_server.trigger_agent(run_c1, "Run C1", dict(run_payload)))
            except run_server.ShutdownRequested:
                run_c1_res["shutdown"] = True

        def run_fake_killpg(pid, sig):
            if run_c2_spawned and pid == run_c2_pgid:
                if sig == signal.SIGTERM:
                    run_c2_signaled.set()
                    run_c2_spawned[0].terminate()
                else:
                    run_c2_spawned[0].kill()
                return True
            if run_c1_spawned and pid == run_c1_pgid:
                if sig == signal.SIGTERM:
                    run_c1_spawned[0].terminate()
                else:
                    run_c1_spawned[0].kill()
                return True
            return False

        def run_fake_pg_exists(pid):
            if run_c2_spawned and pid == run_c2_pgid:
                return run_c2_spawned[0].returncode is None
            if run_c1_spawned and pid == run_c1_pgid:
                return run_c1_spawned[0].returncode is None
            return False

        fake_httpd = mock.MagicMock()

        def fake_serve_forever():
            self.assertTrue(run_entered_gate.wait(timeout=1.5))
            raise KeyboardInterrupt()

        fake_httpd.serve_forever.side_effect = fake_serve_forever

        with (
            mock.patch.object(run_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(run_server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(run_server, "post_trello_comment", return_value=True) as mock_run_post,
            mock.patch.object(run_server, "begin_process_launch", side_effect=run_begin_then_pause),
            mock.patch.object(run_server.asyncio, "create_subprocess_exec", side_effect=run_gate_exec),
            mock.patch.object(run_server, "_kill_process_group", side_effect=run_fake_killpg),
            mock.patch.object(run_server, "_process_group_exists", side_effect=run_fake_pg_exists),
            mock.patch.object(run_server, "requeue_in_flight_cards", return_value=True),
            mock.patch.object(run_server, "start_queue_worker"),
            mock.patch.object(run_server, "start_tailscale_funnel"),
            mock.patch.object(run_server.signal, "signal"),
            mock.patch.object(run_server, "BoundedThreadingHTTPServer", return_value=fake_httpd),
        ):
            t_rc2 = threading.Thread(target=run_c2_worker, daemon=True)
            t_rc1 = None
            t_srv = None
            try:
                t_rc2.start()
                self.assertTrue(run_c2_started.wait(timeout=1.5))
                t_rc1 = threading.Thread(target=run_c1_worker, daemon=True)
                t_srv = threading.Thread(target=lambda: run_server.run(port=0, reconcile_retry_delay=0), daemon=True)
                t_rc1.start()
                t_srv.start()
                self.assertTrue(run_c2_signaled.wait(timeout=0.5))
                t_rc2.join(timeout=1.5)
                self.assertFalse(t_rc2.is_alive())
                mock_run_post.assert_not_called()
                self.assertEqual(run_server.load_sidecar_state()["in_flight_cards"].get(run_c2, {}).get("stage"), "dispatched")
                self.assertTrue(t_srv.is_alive())
                run_release_gate.set()
                t_srv.join(timeout=2.0)
                t_rc1.join(timeout=2.0)
            finally:
                run_c2_started.set()
                run_entered_gate.set()
                run_release_gate.set()
                if t_srv is None:
                    run_server.cleanup_processes()
                for th in (t_srv, t_rc1, t_rc2):
                    if th is not None:
                        th.join(timeout=2.0)
                        self.assertFalse(th.is_alive())
        self.assertFalse(t_srv.is_alive())
        self.assertTrue(run_c1_res.get("shutdown"))
        self.assertEqual(len(run_c1_spawned), 1)
        self.assertTrue(run_c1_spawned[0].terminated or run_c1_spawned[0].killed)

        run_server.shutdown_event.clear()
        run_server.outbound_posts_closed_event.clear()
        run_server.pending_cards.clear()
        self.assertTrue(run_server.requeue_in_flight_cards())
        st_run_rec = run_server.load_sidecar_state()
        self.assertTrue(any(t.get("card_id") == run_c2 for t in st_run_rec.get("unreconciled_tombstones", {}).values()))
        self.assertEqual(
            run_server.should_accept_trigger(run_c2, "Run C2", {"cardId": run_c2, "comment": "External write before shutdown", "triggerUserUsername": "pm_user"}, "External write before shutdown", ""),
            (False, "unreconciled_action"),
        )

        # Startup interruption during Funnel start or HTTP bind failure still cleans up recovered workers.
        for idx_s, startup_mode in enumerate(("funnel_interrupt", "bind_failure")):
            startup_srv = load_server()
            s_card = f"69ddd771111111111111111{idx_s}"
            s_payload = {"cardId": s_card, "comment": f"Startup {startup_mode}", "triggerUserUsername": "pm_user"}
            seed_queued_card(startup_srv, s_card, "Startup Recov", payload=s_payload, admission_id=f"adm-{startup_mode}")
            proc_started = threading.Event()
            proc_signaled = threading.Event()
            s_pgid = os.getpid() + 100010 + idx_s
            s_spawned = []

            async def startup_exec(*cmd, **kwargs):
                proc = FakeStreamProcess(
                    lines=[
                        json.dumps({"event": "init", "conversation_id": f"conv-startup-{idx_s}"}),
                        json.dumps({"event": "tool_use", "name": "bash"}),
                    ],
                    hang_after_lines=True,
                    pid=s_pgid,
                )
                s_spawned.append(proc)
                proc_started.set()
                return proc

            def startup_killpg(pid, sig):
                if s_spawned and pid == s_pgid:
                    if sig == signal.SIGTERM:
                        proc_signaled.set()
                        s_spawned[0].terminate()
                    else:
                        s_spawned[0].kill()
                    return True
                return False

            def startup_pg_exists(pid):
                return bool(s_spawned and pid == s_pgid and s_spawned[0].returncode is None)

            fake_s_httpd = mock.MagicMock()

            def fake_bind(*args, **kwargs):
                self.assertTrue(proc_started.wait(timeout=1.5))
                if startup_mode == "bind_failure":
                    raise OSError("Address already in use")
                return fake_s_httpd

            def fake_funnel(port):
                self.assertTrue(proc_started.wait(timeout=1.5))
                raise KeyboardInterrupt()

            with (
                mock.patch.object(startup_srv, "fetch_card_details_sync", return_value=None),
                mock.patch.object(startup_srv, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(startup_srv, "post_trello_comment", return_value=True) as mock_s_post,
                mock.patch.object(startup_srv.asyncio, "create_subprocess_exec", side_effect=startup_exec),
                mock.patch.object(startup_srv, "_kill_process_group", side_effect=startup_killpg),
                mock.patch.object(startup_srv, "_process_group_exists", side_effect=startup_pg_exists),
                mock.patch.object(startup_srv, "start_tailscale_funnel", side_effect=fake_funnel),
                mock.patch.object(startup_srv.signal, "signal"),
                mock.patch.object(startup_srv, "BoundedThreadingHTTPServer", side_effect=fake_bind),
            ):
                try:
                    if startup_mode == "bind_failure":
                        with self.assertRaises(OSError):
                            startup_srv.run(port=0, reconcile_retry_delay=0)
                    else:
                        startup_srv.run(port=0, reconcile_retry_delay=0)
                        fake_s_httpd.server_close.assert_called_once()
                    startup_srv.executor.shutdown(wait=True)
                finally:
                    proc_started.set()
                    if not startup_srv.shutdown_event.is_set():
                        startup_srv.cleanup_processes()
                    startup_srv.executor.shutdown(wait=True)
            fake_s_httpd.serve_forever.assert_not_called()
            mock_s_post.assert_not_called()
            self.assertTrue(proc_signaled.is_set())
            self.assertTrue(startup_srv.shutdown_event.is_set())
            self.assertTrue(startup_srv.outbound_posts_closed_event.is_set())
            self.assertEqual(len(s_spawned), 1)
            self.assertTrue(s_spawned[0].terminated or s_spawned[0].killed)
            self.assertEqual(startup_srv.load_sidecar_state()["in_flight_cards"].get(s_card, {}).get("stage"), "dispatched")

    def test_deferred_planner_bounded_retry_and_exhaustion_replay(self):
        server = load_server({
            "TRELLO_DEFERRED_RETRY_MAX_ATTEMPTS": "2",
            "TRELLO_DEFERRED_RETRY_DELAY_SECONDS": "0.05",
        })
        card_key = "69eeaa111111111111111111"
        payload = {
            "cardId": card_key,
            "listName": "Ready for Spec",
            "comment": "Spec this",
            "action": {"id": "act-defer-recov", "type": "commentCard", "memberCreator": {"username": "pm_user"}},
        }
        self.assertTrue(server.should_accept_trigger(card_key, "Defer Card", payload, "Spec this", "Ready for Spec")[0])

        # Attempt 1 during outage schedules a timer and preserves queued row + pending_cards.
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "collect_github_work_context") as mock_gh,
            mock.patch.object(server.asyncio, "create_subprocess_exec") as mock_exec,
        ):
            server.run_agent_job(card_key, "Defer Card", copy.deepcopy(payload))
        mock_gh.assert_not_called()
        mock_exec.assert_not_called()
        self.assertIn(card_key, server.pending_cards)
        self.assertEqual(server.load_sidecar_state()["in_flight_cards"][card_key]["defer_attempts"], 1)

        re_key, re_name, re_payload = server.task_queue.get(timeout=1.5)
        self.assertEqual(re_key, card_key)

        # Once Trello recovers, deferred retry runs planner cleanly and releases slot.
        recovered_card = {
            "id": card_key,
            "shortLink": "Recov123",
            "url": "https://trello.com/c/Recov123",
            "name": "Defer Card",
            "desc": "Recovered",
            "list": {"name": "Ready for Spec"},
            "actions": [],
        }

        async def fake_ok(*cmd, **kwargs):
            lines = [
                json.dumps({"event": "init", "conversation_id": "conv-defer-ok"}),
                json.dumps({"event": "result", "result": {"conversation_id": "conv-defer-ok", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=recovered_card),
            mock.patch.object(server, "collect_github_work_context", return_value=""),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_ok),
        ):
            server.run_agent_job(re_key, re_name, re_payload)
        self.assertNotIn(card_key, server.pending_cards)
        self.assertNotIn(card_key, server.load_sidecar_state().get("in_flight_cards", {}))

        # Exhausting DEFERRED_RETRY_MAX_ATTEMPTS clears recent_triggers so same action can replay intentionally.
        card_ex = "69eeaa222222222222222222"
        payload_ex = {
            "cardId": card_ex,
            "listName": "Ready for Spec",
            "comment": "Spec exhaust",
            "action": {"id": "act-defer-exhaust", "type": "commentCard", "memberCreator": {"username": "pm_user"}},
        }
        self.assertTrue(server.should_accept_trigger(card_ex, "Exhaust Card", payload_ex, "Spec exhaust", "Ready for Spec")[0])
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "collect_github_work_context"),
        ):
            server.run_agent_job(card_ex, "Exhaust Card", copy.deepcopy(payload_ex))
            re2_key, re2_name, re2_payload = server.task_queue.get(timeout=1.5)
            server.run_agent_job(re2_key, re2_name, re2_payload)
            re3_key, re3_name, re3_payload = server.task_queue.get(timeout=1.5)
            server.run_agent_job(re3_key, re3_name, re3_payload)

        self.assertNotIn(card_ex, server.pending_cards)
        self.assertEqual(server.load_sidecar_state().get("in_flight_cards"), {})
        replay_ok, replay_reason = server.should_accept_trigger(
            card_ex,
            "Exhaust Card",
            {"cardId": card_ex, "listName": "Ready for Spec", "comment": "Spec exhaust", "action": {"id": "act-defer-exhaust", "type": "commentCard", "memberCreator": {"username": "pm_user"}}},
            "Spec exhaust",
            "Ready for Spec",
        )
        self.assertTrue((replay_ok, replay_reason) == (True, "accepted"))

        # Transient state save failure during defer OR exhaustion retains custody until storage recovers.
        card_fail_save = "69eeaa333333333333333333"
        payload_fail_save = {
            "cardId": card_fail_save,
            "listName": "Ready for Spec",
            "comment": "Spec save fail recovery",
            "action": {"id": "act-defer-save-fail", "type": "commentCard", "memberCreator": {"username": "pm_user"}},
        }
        self.assertTrue(server.should_accept_trigger(card_fail_save, "Save Fail Defer", payload_fail_save, "Spec save fail recovery", "Ready for Spec")[0])
        while not server.task_queue.empty():
            server.task_queue.get_nowait()

        # Fail save on attempt 1 defer: row stays queued and timer still requeues for recovery.
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "save_sidecar_state", return_value=False),
            mock.patch.object(server, "collect_github_work_context"),
        ):
            server.run_agent_job(card_fail_save, "Save Fail Defer", copy.deepcopy(payload_fail_save))
        self.assertIn(card_fail_save, server.pending_cards)
        self.assertIn(card_fail_save, server.load_sidecar_state()["in_flight_cards"])
        q_fs_k, q_fs_n, q_fs_p = server.task_queue.get(timeout=1.5)

        # Fail save at exhaustion, then recover storage: exhaustion cleanup completes cleanly on retry.
        st_ex = server.load_sidecar_state()
        st_ex["in_flight_cards"][card_fail_save]["defer_attempts"] = 2
        self.assertTrue(server.save_sidecar_state(st_ex))
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "save_sidecar_state", return_value=False),
            mock.patch.object(server, "collect_github_work_context"),
        ):
            server.run_agent_job(q_fs_k, q_fs_n, q_fs_p)
        self.assertIn(card_fail_save, server.pending_cards)
        self.assertIn(card_fail_save, server.load_sidecar_state()["in_flight_cards"])
        q_fs_k2, q_fs_n2, q_fs_p2 = server.task_queue.get(timeout=1.5)
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "collect_github_work_context"),
        ):
            server.run_agent_job(q_fs_k2, q_fs_n2, q_fs_p2)
        self.assertNotIn(card_fail_save, server.pending_cards)
        self.assertEqual(
            server.should_accept_trigger(
                card_fail_save,
                "Save Fail Defer",
                {"cardId": card_fail_save, "listName": "Ready for Spec", "comment": "Spec save fail recovery", "action": {"id": "act-defer-save-fail", "type": "commentCard", "memberCreator": {"username": "pm_user"}}},
                "Spec save fail recovery",
                "Ready for Spec",
            ),
            (True, "accepted"),
        )

    def test_prompt_budget_enforces_minimum_and_preserves_real_planner_template(self):
        clamped = load_server({"TRELLO_MAX_PROMPT_BYTES": "100"})
        self.assertEqual(clamped.MAX_PROMPT_BYTES, clamped.MIN_PROMPT_BYTES)

        real_planner_instr = "PLANNER_HEAD " + ("P" * 6500) + " PLANNER_TAIL"
        real_sec_rules = "SEC_HEAD " + ("S" * 600) + " SEC_TAIL"
        escaped_comment = '"\\\n\t' * 500
        payload = {"cardId": "cardKEEP99", "comment": escaped_comment}
        prompt = clamped.build_agent_prompt(
            system_instruction=real_planner_instr,
            security_rules=real_sec_rules,
            payload_str=json.dumps(payload),
            card_details_str="D" * 10000,
            max_bytes=clamped.MIN_PROMPT_BYTES,
        )
        self.assertLessEqual(len(prompt.encode("utf-8")), clamped.MIN_PROMPT_BYTES)
        self.assertIn("PLANNER_HEAD", prompt)
        self.assertIn("PLANNER_TAIL", prompt)
        self.assertIn("SEC_HEAD", prompt)
        self.assertIn("SEC_TAIL", prompt)
        self.assertIn("cardKEEP99", prompt)
        self.assertIn("3. Output your response or spec summary.", prompt)

        with self.assertRaises(ValueError):
            clamped.build_agent_prompt("R" * 500, "S" * 500, json.dumps({"cardId": "x", "comment": "y"}), max_bytes=200)
        with self.assertRaises(ValueError):
            clamped.build_classifier_prompt("comment", "card", "desc", "Inbox", max_bytes=200)

        # Real READY_FOR_SPEC template in trigger_agent at MIN_PROMPT_BYTES with cached link, escaped comment,
        # and no-match preflight preserves full instructions AND verified cardLink/cardShortId.
        cached_card = "69bbbb333333333333333333"
        clamped.record_verified_card_alias("CachedAb", cached_card, card_url="https://trello.com/c/CachedAb")
        seed_queued_card(clamped, cached_card, "Min Budget Spec Card")
        captured_real_prompts = []

        async def fake_real_planner(*cmd, **kwargs):
            captured_real_prompts.append(cmd[-1])
            lines = [
                json.dumps({"event": "init", "conversation_id": "conv-min-real-1"}),
                json.dumps({"event": "result", "result": {"conversation_id": "conv-min-real-1", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        with (
            mock.patch.object(clamped, "fetch_card_details_sync", return_value=None),
            mock.patch.object(clamped, "collect_github_work_context", return_value="No matching GitHub issues found.\n"),
            mock.patch.object(clamped.asyncio, "create_subprocess_exec", side_effect=fake_real_planner),
        ):
            asyncio.run(
                clamped.trigger_agent(
                    cached_card,
                    "Min Budget Spec Card",
                    {
                        "cardId": cached_card,
                        "listName": "Ready for Spec",
                        "comment": '"\\\n\t' * 2000,
                        "action": {"id": "act-min-budget", "type": "commentCard", "memberCreator": {"username": "pm_user"}},
                    },
                )
            )
        self.assertEqual(len(captured_real_prompts), 1)
        real_p = captured_real_prompts[0]
        self.assertLessEqual(len(real_p.encode("utf-8")), clamped.MIN_PROMPT_BYTES)
        self.assertIn("You are reviewing and responding to Trello cards as the PLANNER", real_p)
        self.assertIn("CRITICAL SECURITY REQUIREMENT:", real_p)
        self.assertIn(cached_card, real_p)
        self.assertIn("CachedAb", real_p)
        self.assertIn("https://trello.com/c/CachedAb", real_p)
        self.assertIn("3. Output your response or spec summary.", real_p)

    def test_legacy_mixed_case_session_recovery_and_conflict(self):
        server = load_server()
        upper_key = "69AABB112233445566778899"
        lower_key = upper_key.lower()
        Path(server.SESSION_MAP_FILE).write_text(json.dumps({upper_key: "conv-legacy-upper"}))

        seen_cmds = []

        async def fake_ok(*cmd, **kwargs):
            seen_cmds.append(list(cmd))
            lines = [
                json.dumps({"event": "init", "conversation_id": "conv-legacy-upper"}),
                json.dumps({"event": "result", "result": {"conversation_id": "conv-legacy-upper", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        seed_queued_card(server, lower_key, "Legacy Session Card")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_ok),
        ):
            asyncio.run(server.trigger_agent(lower_key, "Legacy Session Card", {"cardId": lower_key, "comment": "Follow up"}))

        self.assertIn("--conversation", seen_cmds[-1])
        self.assertEqual(seen_cmds[-1][seen_cmds[-1].index("--conversation") + 1], "conv-legacy-upper")
        saved_map = json.loads(Path(server.SESSION_MAP_FILE).read_text())
        self.assertEqual(saved_map.get(lower_key), "conv-legacy-upper")
        self.assertNotIn(upper_key, saved_map)

        # Ambiguous mixed-case canonical duplicates hold cleanly (0 spawns, sessions map untouched across deliveries).
        conflict_lower = "69ccdd112233445566778899"
        initial_conflict_map = {
            "69CCDD112233445566778899": "conv-c1",
            "69ccDD112233445566778899": "conv-c2",
        }
        Path(server.SESSION_MAP_FILE).write_text(json.dumps(initial_conflict_map))
        Path(server.SIDECAR_EVENT_LOG_FILE).write_text("")
        seen_cmds.clear()

        async def fake_fresh(*cmd, **kwargs):
            seen_cmds.append(list(cmd))
            lines = [
                json.dumps({"event": "init", "conversation_id": "conv-fresh-safe"}),
                json.dumps({"event": "result", "result": {"conversation_id": "conv-fresh-safe", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        for delivery in (1, 2):
            seed_queued_card(server, conflict_lower, "Conflict Card")
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_fresh),
            ):
                asyncio.run(server.trigger_agent(conflict_lower, "Conflict Card", {"cardId": conflict_lower, "comment": f"Follow up {delivery}"}))

        self.assertEqual(len(seen_cmds), 0)
        self.assertEqual(json.loads(Path(server.SESSION_MAP_FILE).read_text()), initial_conflict_map)
        events = [json.loads(l) for l in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertTrue(any(e["event"] == "session_alias_conflict" for e in events))
        self.assertTrue(any(e["event"] == "trigger_blocked" and e.get("reason") == "ambiguous_session_alias_conflict" for e in events))

        # End-to-end HTTP -> run_agent_job conflict releases slot and dedup so redelivery runs after repair.
        Path(server.SIDECAR_STATE_FILE).write_text("{}")
        server.pending_cards.clear()
        while not server.task_queue.empty():
            server.task_queue.get_nowait()
        server.AUTH_TOKEN = "secret-webhook-token"
        httpd = server.BoundedThreadingHTTPServer(("127.0.0.1", 0), server.WebhookHandler, max_workers=4)
        port = httpd.server_address[1]
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        conflict_action_body = {
            "cardId": conflict_lower,
            "comment": "Retry after repair",
            "action": {"id": "act-ambig-http-1", "type": "commentCard", "memberCreator": {"username": "pm_user"}},
        }
        try:
            def post_conflict():
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps(conflict_action_body).encode("utf-8"),
                    headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return resp.status, json.loads(resp.read().decode("utf-8"))

            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                self.assertEqual(post_conflict(), (202, {"status": "accepted"}))
            qk_c, qn_c, qp_c = server.task_queue.get_nowait()
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_fresh),
            ):
                server.run_agent_job(qk_c, qn_c, qp_c)
            self.assertEqual(len(seen_cmds), 0)
            self.assertNotIn(conflict_lower, server.pending_cards)
            st_cleared = server.load_sidecar_state()
            self.assertEqual(st_cleared.get("in_flight_cards"), {})
            self.assertFalse(any(rt.get("id") == "act-ambig-http-1" for rt in st_cleared.get("recent_triggers", [])))

            # Repair trello_sessions.json and redeliver the exact same HTTP action -> 202 and resumes repaired session.
            Path(server.SESSION_MAP_FILE).write_text(json.dumps({conflict_lower: "conv-repaired-1"}))
            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                self.assertEqual(post_conflict(), (202, {"status": "accepted"}))
            qk_c2, qn_c2, qp_c2 = server.task_queue.get_nowait()

            async def fake_repaired_resume(*cmd, **kwargs):
                seen_cmds.append(list(cmd))
                return FakeStreamProcess(
                    lines=[
                        json.dumps({"event": "init", "conversation_id": "conv-repaired-1"}),
                        json.dumps({"event": "result", "result": {"conversation_id": "conv-repaired-1", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
                    ],
                    returncode=0,
                )

            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_repaired_resume),
            ):
                server.run_agent_job(qk_c2, qn_c2, qp_c2)
            self.assertEqual(len(seen_cmds), 1)
            self.assertIn("--conversation", seen_cmds[0])
            self.assertEqual(seen_cmds[0][seen_cmds[0].index("--conversation") + 1], "conv-repaired-1")
        finally:
            httpd.shutdown()
            httpd.server_close()

        # Mixed-case legacy canonical mapping beats conflicting short alias AND keeps short alias intact through stale-session retry,
        # and a failed callback read preserves unrelated cards and CachedS1 without overwriting trello_sessions.json.
        Path(server.SIDECAR_EVENT_LOG_FILE).write_text("")
        canon_upper = "69DDEE112233445566778899"
        canon_lower = canon_upper.lower()
        unrelated_card = "69eeee112233445566778899"
        server.record_verified_card_alias("CachedS1", canon_lower, card_url="https://trello.com/c/CachedS1")
        Path(server.SESSION_MAP_FILE).write_text(json.dumps({
            canon_upper: "conv-canon-A",
            "CachedS1": "conv-short-B",
            unrelated_card: "conv-unrelated-keep",
        }))
        seen_cmds.clear()
        orig_open_cb = builtins.open
        orig_exists_cb = os.path.exists
        inject_callback_read_err = {"fail": False}
        should_fail_read = {"on": True}

        async def fake_canon_stale_retry(*cmd, **kwargs):
            seen_cmds.append(list(cmd))
            if "--conversation" in cmd:
                return FakeStreamProcess(
                    lines=[json.dumps({"event": "result", "result": {"status": "ERROR", "error": "Conversation not found"}})],
                    returncode=1,
                )
            if should_fail_read["on"]:
                inject_callback_read_err["fail"] = True
            lines = [
                json.dumps({"event": "init", "conversation_id": "conv-canon-retry-new"}),
                json.dumps({"event": "result", "result": {"conversation_id": "conv-canon-retry-new", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        target_smap = os.path.abspath(server.SESSION_MAP_FILE)

        def fs_open_cb(file, *args, **kwargs):
            mode = args[0] if args else kwargs.get("mode", "r")
            if inject_callback_read_err["fail"] and os.path.abspath(str(file)) == target_smap and "r" in str(mode):
                raise OSError(errno.EIO, "Simulated session map EIO")
            return orig_open_cb(file, *args, **kwargs)

        def fs_exists_cb(path):
            if inject_callback_read_err["fail"] and os.path.abspath(str(path)) == target_smap:
                return False
            return orig_exists_cb(path)

        seed_queued_card(server, canon_lower, "Short Conflict Card")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch("builtins.open", side_effect=fs_open_cb),
            mock.patch("os.path.exists", side_effect=fs_exists_cb),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_canon_stale_retry),
        ):
            asyncio.run(server.trigger_agent(canon_lower, "Short Conflict Card", {"cardId": canon_lower, "comment": "Follow up"}))

        self.assertEqual(len(seen_cmds), 2)
        saved_after_fail = json.loads(Path(server.SESSION_MAP_FILE).read_text())
        self.assertEqual(saved_after_fail.get(unrelated_card), "conv-unrelated-keep")
        self.assertEqual(saved_after_fail.get("CachedS1"), "conv-short-B")
        fail_events = [json.loads(l)["event"] for l in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertIn("conversation_link_failed", fail_events)

        # Once read recovers, retrying updates only canon_lower and preserves unrelated card and CachedS1.
        should_fail_read["on"] = False
        inject_callback_read_err["fail"] = False
        Path(server.SESSION_MAP_FILE).write_text(json.dumps({
            canon_upper: "conv-canon-A",
            "CachedS1": "conv-short-B",
            unrelated_card: "conv-unrelated-keep",
        }))
        seen_cmds.clear()

        seed_queued_card(server, canon_lower, "Short Conflict Card")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_canon_stale_retry),
        ):
            asyncio.run(server.trigger_agent(canon_lower, "Short Conflict Card", {"cardId": canon_lower, "comment": "Follow up"}))

        self.assertEqual(len(seen_cmds), 2)
        self.assertEqual(seen_cmds[0][seen_cmds[0].index("--conversation") + 1], "conv-canon-A")
        self.assertNotIn("--conversation", seen_cmds[1])
        saved_conflict = json.loads(Path(server.SESSION_MAP_FILE).read_text())
        self.assertEqual(saved_conflict.get(canon_lower), "conv-canon-retry-new")
        self.assertEqual(saved_conflict.get("CachedS1"), "conv-short-B")
        self.assertEqual(saved_conflict.get(unrelated_card), "conv-unrelated-keep")
        self.assertNotIn(canon_upper, saved_conflict)
        sc_events = [json.loads(l)["event"] for l in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertIn("session_alias_conflict", sc_events)

        # Actual missing SESSION_MAP_FILE initializes cleanly.
        Path(server.SESSION_MAP_FILE).unlink()
        seed_queued_card(server, "69aaaa000000000000000088", "Fresh Missing Map")
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_canon_stale_retry),
        ):
            asyncio.run(server.trigger_agent("69aaaa000000000000000088", "Fresh Missing Map", {"cardId": "69aaaa000000000000000088", "comment": "Hello"}))
        self.assertEqual(json.loads(Path(server.SESSION_MAP_FILE).read_text()).get("69aaaa000000000000000088"), "conv-canon-retry-new")

        # HTTP admission rejects filesystem EIO even when os.path.exists() lies False, preserving state bytes; missing file initializes cleanly.
        fs_server = load_server()
        fs_server.AUTH_TOKEN = "secret-webhook-token"
        initial_state_obj = {
            "recent_triggers": [{"id": "act-fs-keep", "card": "69aaaa000000000000000077", "seen_at": time.time()}],
            "card_activity": {},
        }
        Path(fs_server.SIDECAR_STATE_FILE).write_text(json.dumps(initial_state_obj))
        state_bytes_before = Path(fs_server.SIDECAR_STATE_FILE).read_bytes()
        target_state_path = os.path.abspath(fs_server.SIDECAR_STATE_FILE)
        state_err_flag = {"on": True}

        def fs_open_state(file, *args, **kwargs):
            mode = args[0] if args else kwargs.get("mode", "r")
            if state_err_flag["on"] and os.path.abspath(str(file)) == target_state_path and "r" in str(mode):
                raise OSError(errno.EIO, "Simulated sidecar state EIO")
            return orig_open_cb(file, *args, **kwargs)

        def fs_exists_state(path):
            if state_err_flag["on"] and os.path.abspath(str(path)) == target_state_path:
                return False
            return orig_exists_cb(path)

        fs_httpd = fs_server.BoundedThreadingHTTPServer(("127.0.0.1", 0), fs_server.WebhookHandler, max_workers=4)
        fs_port = fs_httpd.server_address[1]
        fs_thread = threading.Thread(target=fs_httpd.serve_forever, daemon=True)
        fs_thread.start()
        try:
            fs_body = {"cardId": "69aaaa000000000000000099", "comment": "Check state IO", "action": {"id": "act-fs-new", "type": "commentCard"}}
            with (
                mock.patch.object(fs_server, "fetch_card_details_sync", return_value=None),
                mock.patch("builtins.open", side_effect=fs_open_state),
                mock.patch("os.path.exists", side_effect=fs_exists_state),
            ):
                req_503 = urllib.request.Request(
                    f"http://127.0.0.1:{fs_port}/",
                    method="POST",
                    data=json.dumps(fs_body).encode("utf-8"),
                    headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx_503:
                    urllib.request.urlopen(req_503, timeout=5)
                self.assertEqual(ctx_503.exception.code, 503)
                ctx_503.exception.close()
            self.assertTrue(fs_server.task_queue.empty())
            self.assertEqual(Path(fs_server.SIDECAR_STATE_FILE).read_bytes(), state_bytes_before)

            state_err_flag["on"] = False
            Path(fs_server.SIDECAR_STATE_FILE).unlink()
            with mock.patch.object(fs_server, "fetch_card_details_sync", return_value=None):
                req_202 = urllib.request.Request(
                    f"http://127.0.0.1:{fs_port}/",
                    method="POST",
                    data=json.dumps(fs_body).encode("utf-8"),
                    headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req_202, timeout=5) as resp_202:
                    self.assertEqual(resp_202.status, 202)
            self.assertEqual(fs_server.task_queue.get_nowait()[0], "69aaaa000000000000000099")
        finally:
            fs_httpd.shutdown()
            fs_httpd.server_close()

    def test_ack_refused_and_never_posts_when_admission_row_missing(self):
        server = load_server({"TRELLO_POST_ACK_COMMENTS": "true"})
        card_key = "69ffff222222222222222222"
        payload = {
            "cardId": card_key,
            "comment": "Investigate carefully",
            "action": {"id": "act-missing-ack", "type": "commentCard", "memberCreator": {"username": "pm_user"}},
        }
        self.assertTrue(server.should_accept_trigger(card_key, "Missing Ack Card", payload, "Investigate carefully", "Ideas")[0])

        # Delete admission row after admission, preserving other state cleanly.
        state = server.load_sidecar_state()
        state["in_flight_cards"].clear()
        self.assertTrue(server.save_sidecar_state(state))

        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="INVESTIGATOR")),
            mock.patch.object(server, "post_trello_comment") as mock_post,
            mock.patch.object(server.asyncio, "create_subprocess_exec") as mock_exec,
        ):
            with self.assertRaises(RuntimeError):
                asyncio.run(server.trigger_agent(card_key, "Missing Ack Card", dict(payload)))

        mock_post.assert_not_called()
        mock_exec.assert_not_called()
        ack_events = [json.loads(l) for l in Path(server.SIDECAR_EVENT_LOG_FILE).read_text().splitlines()]
        self.assertTrue(any(e["event"] == "ack_reservation_failed" for e in ack_events))

    def test_non_comment_move_strips_stale_nested_action_text(self):
        server = load_server()
        server.AUTH_TOKEN = "secret-webhook-token"
        card_key = "69eeee222222222222222222"
        live_card = {
            "id": card_key,
            "shortLink": "MoveNested",
            "url": "https://trello.com/c/MoveNested",
            "name": "Move Nested Card",
            "desc": "Card description",
            "list": {"name": "In Progress"},
            "actions": [
                {
                    "id": "act-live-discussion",
                    "type": "commentCard",
                    "date": "2026-11-19T00:00:00Z",
                    "data": {"text": "LIVE_DISCUSSION_KEEP"},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                }
            ],
        }
        captured_prompts = []

        async def fake_move_exec(*cmd, **kwargs):
            captured_prompts.append(cmd[-1])
            lines = [
                json.dumps({"event": "init", "conversation_id": "conv-move-nested"}),
                json.dumps({"event": "result", "result": {"conversation_id": "conv-move-nested", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        httpd = server.BoundedThreadingHTTPServer(("127.0.0.1", 0), server.WebhookHandler, max_workers=4)
        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=live_card),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="INVESTIGATOR")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_move_exec),
            ):
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}/",
                    method="POST",
                    data=json.dumps({
                        "cardId": card_key,
                        "comment": "STALE_TOP_TEXT",
                        "action": {
                            "id": "act-move-nested",
                            "type": "updateCard",
                            "data": {
                                "card": {"id": card_key, "shortLink": "MoveNested"},
                                "text": "STALE_NESTED_TEXT",
                                "listAfter": {"name": "In Progress"},
                            },
                            "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                        },
                    }).encode("utf-8"),
                    headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    self.assertEqual(resp.status, 202)
                q_key, q_name, q_payload = server.task_queue.get_nowait()
                self.assertNotIn("comment", q_payload)
                self.assertNotIn("text", q_payload["action"]["data"])
                server.run_agent_job(q_key, q_name, q_payload)
        finally:
            httpd.shutdown()
            httpd.server_close()

        self.assertEqual(len(captured_prompts), 1)
        self.assertNotIn("STALE_TOP_TEXT", captured_prompts[0])
        self.assertNotIn("STALE_NESTED_TEXT", captured_prompts[0])
        self.assertIn("LIVE_DISCUSSION_KEEP", captured_prompts[0])

    def test_ack_comment_is_idempotent_across_restart_and_cooldown(self):
        server = load_server({"TRELLO_POST_ACK_COMMENTS": "true"})
        card_key = "69ffff111111111111111111"
        payload = {
            "cardId": card_key,
            "comment": "Please investigate deeply",
            "action": {"id": "act-ack-1", "type": "commentCard", "memberCreator": {"username": "pm_user"}},
        }
        seed_queued_card(server, card_key, "Ack Card", payload=payload)
        self.assertTrue(server.reserve_ack_comment(card_key, "INVESTIGATOR", action_id="act-ack-1"))

        # Simulate crash before dispatch and restart recovery.
        server.pending_cards.clear()
        while not server.task_queue.empty():
            server.task_queue.get_nowait()
        server.requeue_in_flight_cards()
        queued_key, _, queued_payload = server.task_queue.get_nowait()
        self.assertEqual(queued_key, card_key)

        # Age last_ack_by_card past cooldown to verify action/entry idempotency still blocks re-ack.
        state = server.load_sidecar_state()
        state["last_ack_by_card"][card_key] = time.time() - (server.ACK_COOLDOWN_SECONDS + 60)
        self.assertTrue(server.save_sidecar_state(state))

        async def fake_ok_exec(*cmd, **kwargs):
            lines = [
                json.dumps({"event": "init", "conversation_id": "conv-ack-1"}),
                json.dumps({
                    "event": "result",
                    "result": {"conversation_id": "conv-ack-1", "status": "SUCCESS", "response": "Done", "usage": {"total_tokens": 5}},
                }),
            ]
            return FakeStreamProcess(lines=lines, returncode=0)

        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="INVESTIGATOR")),
            mock.patch.object(server, "post_trello_comment") as mock_ack_post,
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_ok_exec),
        ):
            asyncio.run(server.trigger_agent(card_key, "Ack Card", dict(queued_payload)))

        mock_ack_post.assert_not_called()

        # ID-less relay payload posts ack once before shutdown, preserves ack_reserved across restart,
        # and does not post a second ack even after cooldown expires.
        idless_server = load_server({"TRELLO_POST_ACK_COMMENTS": "true"})
        idless_card = "69ffff333333333333333333"
        idless_payload = {
            "cardId": idless_card,
            "comment": "Investigate ID-less card",
            "triggerUserUsername": "pm_user",
        }
        self.assertTrue(idless_server.should_accept_trigger(idless_card, "IDless Ack", idless_payload, "Investigate ID-less card", "Ideas")[0])
        ack_calls = []

        def ack_then_shutdown_later(cid, txt, kind="comment"):
            ack_calls.append((cid, txt, kind))
            return True

        with (
            mock.patch.object(idless_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(idless_server, "classify_action", side_effect=mock.AsyncMock(return_value="INVESTIGATOR")),
            mock.patch.object(idless_server, "post_trello_comment", side_effect=ack_then_shutdown_later),
            mock.patch.object(idless_server, "begin_process_launch", side_effect=lambda: (idless_server.initiate_shutdown(), False)[1]),
        ):
            with self.assertRaises(idless_server.ShutdownRequested):
                asyncio.run(idless_server.trigger_agent(idless_card, "IDless Ack", copy.deepcopy(idless_payload)))
        self.assertEqual(len(ack_calls), 1)

        idless_server.shutdown_event.clear()
        idless_server.pending_cards.clear()
        while not idless_server.task_queue.empty():
            idless_server.task_queue.get_nowait()
        self.assertTrue(idless_server.requeue_in_flight_cards())
        q_k, _, q_p = idless_server.task_queue.get_nowait()
        st_idless = idless_server.load_sidecar_state()
        st_idless["last_ack_by_card"][idless_card] = time.time() - (idless_server.ACK_COOLDOWN_SECONDS + 60)
        self.assertTrue(idless_server.save_sidecar_state(st_idless))

        with (
            mock.patch.object(idless_server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(idless_server, "classify_action", side_effect=mock.AsyncMock(return_value="INVESTIGATOR")),
            mock.patch.object(idless_server, "post_trello_comment", side_effect=ack_then_shutdown_later),
            mock.patch.object(idless_server.asyncio, "create_subprocess_exec", side_effect=fake_ok_exec),
        ):
            asyncio.run(idless_server.trigger_agent(q_k, "IDless Ack", copy.deepcopy(q_p)))
        self.assertEqual(len(ack_calls), 1)

    def test_tailscale_funnel_is_opt_in_by_default(self):
        server = load_server()
        self.assertFalse(server.ENABLE_TAILSCALE_FUNNEL)
        with mock.patch.object(server.subprocess, "Popen") as mock_popen:
            server.start_tailscale_funnel(8454)
        mock_popen.assert_not_called()

    def test_idless_outage_recovery_orders_head_upgrade_and_distinct_fingerprints(self):
        server = load_server({
            "TRELLO_POST_ACK_COMMENTS": "true",
            "TRELLO_SECRET": "sec-r7",
            "TRELLO_WEBHOOK_CALLBACK_URL": "https://example.com/trello-webhook",
        })
        server.AUTH_TOKEN = "secret-webhook-token"
        httpd = server.BoundedThreadingHTTPServer(("127.0.0.1", 0), server.WebhookHandler, max_workers=4)
        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()

        def post_json(path, body_obj, headers=None):
            hdrs = {"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json"}
            if headers is not None:
                hdrs = headers
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}{path}",
                method="POST",
                data=json.dumps(body_obj).encode("utf-8"),
                headers=hdrs,
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))

        try:
            # 1. Order A: authorless ID-less HTTP relay hits outage -> interrupted -> Trello recovers with actionId + author -> refused (1 launch).
            card_a = "69aa71000000000000000001"
            raw_body_a = {"cardId": card_a, "comment": "Deploy Order A"}
            live_a = {
                "id": card_a,
                "shortLink": "OrdA1111",
                "url": "https://trello.com/c/OrdA1111",
                "name": "Order A Card",
                "desc": "Desc",
                "list": {"name": "Ideas"},
                "actions": [
                    {
                        "id": "act-order-a",
                        "type": "commentCard",
                        "date": "2026-11-19T00:00:00Z",
                        "data": {"text": "Deploy Order A"},
                        "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                    }
                ],
            }
            spawns_a = []
            timers_a = []

            async def fake_interrupt_exec_a(*cmd, **kwargs):
                spawns_a.append(list(cmd))
                proc = FakeStreamProcess(lines=[json.dumps({"event": "init", "conversation_id": "conv-oa-1"})], hang_after_lines=True)
                t_a = threading.Timer(0.05, server.cleanup_processes)
                timers_a.append(t_a)
                t_a.start()
                return proc

            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                self.assertEqual(post_json("/", raw_body_a), (202, {"status": "accepted"}))
            qk_a, _, qp_a = server.task_queue.get_nowait()
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=None),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_interrupt_exec_a),
            ):
                try:
                    server.run_agent_job(qk_a, "Order A Card", qp_a)
                finally:
                    for t_a in timers_a:
                        t_a.cancel()
                        t_a.join(timeout=2.0)
                        self.assertFalse(t_a.is_alive())
            self.assertEqual(len(spawns_a), 1)
            server.shutdown_event.clear()
            server.outbound_posts_closed_event.clear()
            server.pending_cards.clear()
            self.assertTrue(server.requeue_in_flight_cards())

            # Clear ordinary cooldown/recent_triggers so only permanent tombstone guards replay.
            st_a = server.load_sidecar_state()
            st_a["recent_triggers"] = []
            st_a["card_activity"] = {}
            self.assertTrue(server.save_sidecar_state(st_a))

            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_a):
                st, bdy = post_json("/", raw_body_a)
            self.assertEqual((st, bdy), (200, {"status": "ignored", "reason": "unreconciled_action"}))
            self.assertTrue(server.task_queue.empty())
            self.assertEqual(len(spawns_a), 1)

            # 2. Order B: authorless ID-less HTTP relay hits live recovery (learns actionId + author) -> interrupted -> outage replay -> refused (1 launch).
            Path(server.SIDECAR_STATE_FILE).write_text("{}")
            card_b = "69aa72000000000000000002"
            raw_body_b = {"cardId": card_b, "comment": "Deploy Order B"}
            live_b = {
                "id": card_b,
                "shortLink": "OrdB2222",
                "url": "https://trello.com/c/OrdB2222",
                "name": "Order B Card",
                "desc": "Desc",
                "list": {"name": "Ideas"},
                "actions": [
                    {
                        "id": "act-order-b",
                        "type": "commentCard",
                        "date": "2026-11-19T00:00:00Z",
                        "data": {"text": "Deploy Order B"},
                        "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                    }
                ],
            }
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_b):
                self.assertEqual(post_json("/", raw_body_b), (202, {"status": "accepted"}))
            qk_b, _, qp_b = server.task_queue.get_nowait()
            self.assertTrue(server.mark_in_flight_dispatched(qk_b, admission_id=qp_b["_admission_id"], action_id=qp_b.get("actionId")))
            server.pending_cards.clear()
            self.assertTrue(server.requeue_in_flight_cards())

            st_b = server.load_sidecar_state()
            st_b["recent_triggers"] = []
            st_b["card_activity"] = {}
            self.assertTrue(server.save_sidecar_state(st_b))

            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                st2, bdy2 = post_json("/", raw_body_b)
            self.assertEqual((st2, bdy2), (200, {"status": "ignored", "reason": "unreconciled_action"}))
            self.assertTrue(server.task_queue.empty())

            # 2b. Same-author same-text comments: replay of interrupted #1 is blocked even after #2 appears,
            # while distinct authoritative #2 and distinct keyed/different-author deliveries under Trello lag still run.
            Path(server.SIDECAR_STATE_FILE).write_text("{}")
            server.pending_cards.clear()
            card_sa = "69aa72500000000000000001"
            raw_sa_body = {"cardId": card_sa, "comment": "Same author text"}
            live_sa_1 = {
                "id": card_sa,
                "shortLink": "SameA111",
                "url": "https://trello.com/c/SameA111",
                "name": "Same Author Card",
                "desc": "Desc",
                "list": {"name": "Ideas"},
                "actions": [
                    {
                        "id": "act-sa-1",
                        "type": "commentCard",
                        "date": "2026-11-19T00:00:00Z",
                        "data": {"text": "Same author text"},
                        "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                    }
                ],
            }
            live_sa_both = {
                **live_sa_1,
                "actions": [
                    {
                        "id": "act-sa-2",
                        "type": "commentCard",
                        "date": "2026-11-19T00:05:00Z",
                        "data": {"text": "Same author text"},
                        "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                    },
                    *live_sa_1["actions"],
                ],
            }
            live_sa_only_2 = {**live_sa_1, "actions": [live_sa_both["actions"][0]]}
            sa_spawns = []
            timers_sa = []

            async def fake_sa_exec(*cmd, **kwargs):
                sa_spawns.append(list(cmd))
                if len(sa_spawns) == 1:
                    proc = FakeStreamProcess(lines=[json.dumps({"event": "init", "conversation_id": "conv-sa-1"})], hang_after_lines=True)
                    t_sa = threading.Timer(0.05, server.cleanup_processes)
                    timers_sa.append(t_sa)
                    t_sa.start()
                    return proc
                return FakeStreamProcess(
                    lines=[
                        json.dumps({"event": "init", "conversation_id": f"conv-sa-{len(sa_spawns)}"}),
                        json.dumps({"event": "result", "result": {"conversation_id": f"conv-sa-{len(sa_spawns)}", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
                    ],
                    returncode=0,
                )

            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_1):
                self.assertEqual(post_json("/", raw_sa_body), (202, {"status": "accepted"}))
            qk_sa, qn_sa, qp_sa = server.task_queue.get_nowait()
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_1),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_sa_exec),
            ):
                try:
                    server.run_agent_job(qk_sa, qn_sa, qp_sa)
                finally:
                    for t_sa in timers_sa:
                        t_sa.cancel()
                        t_sa.join(timeout=2.0)
                        self.assertFalse(t_sa.is_alive())
            self.assertEqual(len(sa_spawns), 1)
            server.shutdown_event.clear()
            server.outbound_posts_closed_event.clear()
            server.pending_cards.clear()
            self.assertTrue(server.requeue_in_flight_cards())

            st_sa = server.load_sidecar_state()
            st_sa["recent_triggers"] = []
            st_sa["card_activity"] = {}
            self.assertTrue(server.save_sidecar_state(st_sa))

            # Replay of interrupted Delivery 1 is blocked whether live Trello returns [act-sa-2, act-sa-1] or only [act-sa-2].
            for live_view in (live_sa_both, live_sa_only_2):
                with mock.patch.object(server, "fetch_card_details_sync", return_value=live_view):
                    self.assertEqual(post_json("/", raw_sa_body), (200, {"status": "ignored", "reason": "unreconciled_action"}))
            self.assertTrue(server.task_queue.empty())
            self.assertEqual(len(sa_spawns), 1)

            # Distinct Delivery 2 carrying authoritative action.id="act-sa-2" is accepted and launches.
            distinct_sa_2 = {
                "cardId": card_sa,
                "action": {
                    "id": "act-sa-2",
                    "type": "commentCard",
                    "data": {"text": "Same author text"},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                },
            }
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_both):
                self.assertEqual(post_json("/", distinct_sa_2), (202, {"status": "accepted"}))
            qk_sa2, qn_sa2, qp_sa2 = server.task_queue.get_nowait()
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_both),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_sa_exec),
            ):
                server.run_agent_job(qk_sa2, qn_sa2, qp_sa2)
            self.assertEqual(len(sa_spawns), 2)

            # Distinct X-Delivery-Id AND distinct author do not inherit quarantined older text match under Trello lag.
            Path(server.SIDECAR_STATE_FILE).write_text("{}")
            server.pending_cards.clear()
            card_sa_k = "69aa72500000000000000002"
            live_sa_k1 = {
                "id": card_sa_k,
                "shortLink": "SameK111",
                "url": "https://trello.com/c/SameK111",
                "name": "Same Keyed Card",
                "desc": "Desc",
                "list": {"name": "Ideas"},
                "actions": [
                    {
                        "id": "act-sa-k1",
                        "type": "commentCard",
                        "date": "2026-11-19T00:00:00Z",
                        "data": {"text": "Lagged same text"},
                        "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                    }
                ],
            }
            hdr_k1 = {"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json", "X-Delivery-Id": "del-sa-1"}
            hdr_k2 = {"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json", "X-Delivery-Id": "del-sa-2"}
            hdr_k3 = {"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json", "X-Delivery-Id": "del-sa-3"}
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_k1):
                self.assertEqual(post_json("/", {"cardId": card_sa_k, "comment": "Lagged same text"}, headers=hdr_k1), (202, {"status": "accepted"}))
            k_sa1, _, p_sa1 = server.task_queue.get_nowait()
            self.assertTrue(server.mark_in_flight_dispatched(k_sa1, admission_id=p_sa1["_admission_id"], action_id=p_sa1.get("actionId"), action_id_inferred=True))
            server.pending_cards.clear()
            self.assertTrue(server.requeue_in_flight_cards())

            # Age cooldown while KEEPING recent_triggers intact; authoritative Delivery 2 with the exact inferred ID and disjoint key launches once, while Delivery 1 replay launches zero times.
            st_sak = server.load_sidecar_state()
            self.assertTrue(any(rt.get("id") == "act-sa-k1" for rt in st_sak.get("recent_triggers", [])))
            st_sak["card_activity"][card_sa_k]["last_seen"] = time.time() - (server.TRIGGER_COOLDOWN_SECONDS + 60)
            self.assertTrue(server.save_sidecar_state(st_sak))

            auth_k2_body = {
                "cardId": card_sa_k,
                "action": {
                    "id": "act-sa-k1",
                    "type": "commentCard",
                    "data": {"text": "Lagged same text"},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                },
            }
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_k1):
                self.assertEqual(post_json("/", auth_k2_body, headers=hdr_k2), (202, {"status": "accepted"}))
                k_sa2, n_sa2, p_sa2 = server.task_queue.get_nowait()
                with (
                    mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                    mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_sa_exec),
                ):
                    server.run_agent_job(k_sa2, n_sa2, p_sa2)
                self.assertEqual(len(sa_spawns), 3)
                self.assertEqual(post_json("/", {"cardId": card_sa_k, "comment": "Lagged same text"}, headers=hdr_k1), (200, {"status": "ignored", "reason": "unreconciled_action"}))
            self.assertTrue(server.task_queue.empty())
            self.assertEqual(len(sa_spawns), 3)

            # Distinct X-Delivery-Id ID-less and distinct author under aged cooldown still launch.
            st_sak2 = server.load_sidecar_state()
            st_sak2["card_activity"][card_sa_k]["last_seen"] = time.time() - (server.TRIGGER_COOLDOWN_SECONDS + 60)
            self.assertTrue(server.save_sidecar_state(st_sak2))
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_k1):
                self.assertEqual(post_json("/", {"cardId": card_sa_k, "comment": "Lagged same text"}, headers=hdr_k3), (202, {"status": "accepted"}))
            k_sa3, n_sa3, p_sa3 = server.task_queue.get_nowait()
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_k1),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_sa_exec),
            ):
                server.run_agent_job(k_sa3, n_sa3, p_sa3)
            self.assertEqual(len(sa_spawns), 4)

            st_sak3 = server.load_sidecar_state()
            st_sak3["card_activity"] = {}
            self.assertTrue(server.save_sidecar_state(st_sak3))
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_k1):
                self.assertEqual(post_json("/", {"cardId": card_sa_k, "comment": "Lagged same text", "triggerUserUsername": "qa_user"}), (202, {"status": "accepted"}))
            k_sa4, n_sa4, p_sa4 = server.task_queue.get_nowait()
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_k1),
                mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_sa_exec),
            ):
                server.run_agent_job(k_sa4, n_sa4, p_sa4)
            self.assertEqual(len(sa_spawns), 5)

            # Keyed inferred tombstone + recent_triggers also does not block a keyless authoritative delivery of that same action ID.
            card_sa_nk = "69aa72500000000000000003"
            live_sa_nk = {
                **live_sa_k1,
                "id": card_sa_nk,
                "shortLink": "SameNK33",
                "url": "https://trello.com/c/SameNK33",
                "actions": [{
                    "id": "act-sa-nk1",
                    "type": "commentCard",
                    "date": "2026-11-19T00:00:00Z",
                    "data": {"text": "Lagged keyless auth"},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                }],
            }
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_nk):
                self.assertEqual(post_json("/", {"cardId": card_sa_nk, "comment": "Lagged keyless auth"}, headers=hdr_k1), (202, {"status": "accepted"}))
            k_nk1, _, p_nk1 = server.task_queue.get_nowait()
            self.assertTrue(server.mark_in_flight_dispatched(k_nk1, admission_id=p_nk1["_admission_id"], action_id=p_nk1.get("actionId"), action_id_inferred=True))
            server.pending_cards.clear()
            self.assertTrue(server.requeue_in_flight_cards())
            st_nk = server.load_sidecar_state()
            st_nk["card_activity"][card_sa_nk]["last_seen"] = time.time() - (server.TRIGGER_COOLDOWN_SECONDS + 60)
            self.assertTrue(server.save_sidecar_state(st_nk))
            auth_nk_body = {
                "cardId": card_sa_nk,
                "action": {
                    "id": "act-sa-nk1",
                    "type": "commentCard",
                    "data": {"text": "Lagged keyless auth"},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                },
            }
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_sa_nk):
                self.assertEqual(post_json("/", auth_nk_body), (202, {"status": "accepted"}))
                k_nk2, n_nk2, p_nk2 = server.task_queue.get_nowait()
                with (
                    mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
                    mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_sa_exec),
                ):
                    server.run_agent_job(k_nk2, n_nk2, p_nk2)
                self.assertEqual(len(sa_spawns), 6)
                self.assertEqual(post_json("/", {"cardId": card_sa_nk, "comment": "Lagged keyless auth"}, headers=hdr_k1), (200, {"status": "ignored", "reason": "unreconciled_action"}))

            # Pruned tombstone with inferred action X does NOT get overwritten if authoritative delivery B with action X interrupts later; both keep distinct record IDs.
            Path(server.SIDECAR_STATE_FILE).write_text("{}")
            server.pending_cards.clear()
            card_pr = "69aa72500000000000000004"
            live_pr = {
                **live_sa_k1,
                "id": card_pr,
                "shortLink": "Prune444",
                "url": "https://trello.com/c/Prune444",
                "actions": [{
                    "id": "act-pr-x",
                    "type": "commentCard",
                    "date": "2026-11-19T00:00:00Z",
                    "data": {"text": "Prune collision text"},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                }],
            }
            hdr_pr_a = {"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json", "X-Delivery-Id": "del-pr-a"}
            hdr_pr_b = {"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json", "X-Delivery-Id": "del-pr-b"}
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_pr):
                self.assertEqual(post_json("/", {"cardId": card_pr, "comment": "Prune collision text"}, headers=hdr_pr_a), (202, {"status": "accepted"}))
            k_pa, _, p_pa = server.task_queue.get_nowait()
            self.assertTrue(server.mark_in_flight_dispatched(k_pa, admission_id=p_pa["_admission_id"], action_id=p_pa.get("actionId"), action_id_inferred=True))
            server.pending_cards.clear()
            self.assertTrue(server.requeue_in_flight_cards())

            st_pr_pad = server.load_sidecar_state()
            for i in range(505):
                st_pr_pad["unreconciled_cards"][f"69dd00{i:018d}:act-pr-pad-{i}"] = {"card_id": f"69dd00{i:018d}", "action_id": f"act-pr-pad-{i}", "stage": "dispatched"}
            st_pr_pad["card_activity"][card_pr]["last_seen"] = time.time() - (server.TRIGGER_COOLDOWN_SECONDS + 60)
            self.assertTrue(server.save_sidecar_state(st_pr_pad))
            self.assertTrue(server.requeue_in_flight_cards())

            st_pr_mid = server.load_sidecar_state()
            self.assertFalse(any(e.get("card_id") == card_pr for e in st_pr_mid["unreconciled_cards"].values()))
            rec_pr_a = next(k for k, t in st_pr_mid["unreconciled_tombstones"].items() if t.get("card_id") == card_pr)

            auth_pr_b = {
                "cardId": card_pr,
                "action": {
                    "id": "act-pr-x",
                    "type": "commentCard",
                    "data": {"text": "Prune collision text"},
                    "memberCreator": {"username": "pm_user", "fullName": "PM User"},
                },
            }
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_pr):
                self.assertEqual(post_json("/", auth_pr_b, headers=hdr_pr_b), (202, {"status": "accepted"}))
            k_pb, _, p_pb = server.task_queue.get_nowait()
            self.assertTrue(server.mark_in_flight_dispatched(k_pb, admission_id=p_pb["_admission_id"], action_id="act-pr-x", action_id_inferred=False))
            server.pending_cards.clear()
            self.assertTrue(server.requeue_in_flight_cards())

            st_pr_both = server.load_sidecar_state()
            pr_tombs = {k: t for k, t in st_pr_both["unreconciled_tombstones"].items() if t.get("card_id") == card_pr}
            self.assertEqual(len(pr_tombs), 2)
            self.assertIn(rec_pr_a, pr_tombs)
            self.assertIn(f"{card_pr}:del-pr-a", pr_tombs[rec_pr_a]["delivery_keys"])
            rec_pr_b = next(k for k in pr_tombs if k != rec_pr_a)
            self.assertIn(f"{card_pr}:del-pr-b", pr_tombs[rec_pr_b]["delivery_keys"])

            st_pr_both["card_activity"] = {}
            self.assertTrue(server.save_sidecar_state(st_pr_both))
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_pr):
                self.assertEqual(post_json("/", {"cardId": card_pr, "comment": "Prune collision text"}, headers=hdr_pr_a), (200, {"status": "ignored", "reason": "unreconciled_action"}))
                self.assertEqual(post_json("/", auth_pr_b, headers=hdr_pr_b), (200, {"status": "ignored", "reason": "unreconciled_action"}))

            st_pa_clr, bdy_pa_clr = post_json("/admin/reconcile", {"recordId": rec_pr_a})
            self.assertEqual(st_pa_clr, 200)
            self.assertEqual(bdy_pa_clr["cleared"], 1)
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_pr):
                self.assertEqual(post_json("/", {"cardId": card_pr, "comment": "Prune collision text"}, headers=hdr_pr_a), (202, {"status": "accepted"}))
                server.release_pending_card(server.task_queue.get_nowait()[0])
                st_pr_cool = server.load_sidecar_state()
                st_pr_cool["card_activity"] = {}
                self.assertTrue(server.save_sidecar_state(st_pr_cool))
                self.assertEqual(post_json("/", auth_pr_b, headers=hdr_pr_b), (200, {"status": "ignored", "reason": "unreconciled_action"}))

            # 3. Worker-learned quarantined action ID is blocked before ack and responder spawn, and refused at dispatch checkpoint.
            Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({
                "unreconciled_tombstones": {
                    "69aa73000000000000000003:act-w-q": {
                        "record_id": "69aa73000000000000000003:act-w-q",
                        "card_id": "69aa73000000000000000003",
                        "action_id": "act-w-q",
                        "fingerprints": [],
                        "legacy_fingerprints": [],
                        "interrupted_at": 1.0,
                    }
                }
            }))
            card_w = "69aa73000000000000000003"
            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                self.assertEqual(post_json("/", {"cardId": card_w, "comment": "Different ID-less text"}), (202, {"status": "accepted"}))
            qk_w, _, qp_w = server.task_queue.get_nowait()
            live_w = {
                "id": card_w,
                "shortLink": "WkrQ3333",
                "url": "https://trello.com/c/WkrQ3333",
                "name": "Worker Recheck",
                "desc": "Desc",
                "list": {"name": "Ideas"},
                "actions": [{"id": "act-w-q", "type": "commentCard", "date": "2026-11-19T00:00:00Z", "data": {"text": "Different ID-less text"}, "memberCreator": {"username": "pm_user"}}],
            }
            with (
                mock.patch.object(server, "fetch_card_details_sync", return_value=live_w),
                mock.patch.object(server, "post_trello_comment") as mock_w_ack,
                mock.patch.object(server.asyncio, "create_subprocess_exec") as mock_w_exec,
            ):
                server.run_agent_job(qk_w, "Worker Recheck", qp_w)
            mock_w_ack.assert_not_called()
            mock_w_exec.assert_not_called()

            seed_queued_card(server, card_w, "Checkpoint Quarantine", admission_id="adm-w-cp")
            cp_detail = {}
            self.assertFalse(server.mark_in_flight_dispatched(card_w, admission_id="adm-w-cp", action_id="act-w-q", detail=cp_detail))
            self.assertEqual(cp_detail.get("status"), "quarantined")

            # 4. HEAD-shaped short-key in_flight row (no stage/action/fingerprint, empty card_aliases) quarantined on upgrade
            # still blocks original link-only HTTP body after canonical long-ID lookup succeeds.
            head_short = "Sh0rtHead7"
            head_canon = "69aa74000000000000000004"
            head_link_body = {"cardLink": f"https://trello.com/c/{head_short}", "comment": "Legacy short link trigger"}
            Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({
                "in_flight_cards": {
                    head_short: {
                        "card_name": "Unnamed Card",
                        "payload": {"cardLink": f"https://trello.com/c/{head_short}", "cardId": head_short, "comment": "Legacy short link trigger"},
                        "queued_at": 1700000000.0,
                    }
                }
            }))
            server.pending_cards.clear()
            self.assertTrue(server.requeue_in_flight_cards())
            live_head = {"id": head_canon, "shortLink": head_short, "url": f"https://trello.com/c/{head_short}", "name": "Upgraded", "desc": "", "list": {"name": "Ideas"}, "actions": []}
            with mock.patch.object(server, "fetch_card_details_sync", return_value=live_head):
                st_h, bdy_h = post_json("/", head_link_body)
                st_hh, bdy_hh = post_json(
                    "/",
                    head_link_body,
                    headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json", "X-Delivery-Id": "head-orig-del"},
                )
            self.assertEqual((st_h, bdy_h), (200, {"status": "ignored", "reason": "unreconciled_action"}))
            self.assertEqual((st_hh, bdy_hh), (200, {"status": "ignored", "reason": "unreconciled_action"}))
            self.assertTrue(server.task_queue.empty())

            # HEAD-shaped stage-less updateCard move with only action.data.listAfter.name (no top-level listName / deliveryId)
            # also blocks byte-identical replay (with or without X-Delivery-Id) after upgrade.
            head_move_canon = "69aa74000000000000000005"
            head_move_body = {
                "cardId": head_move_canon,
                "action": {
                    "type": "updateCard",
                    "data": {
                        "listAfter": {"name": "Ready for Spec"},
                        "card": {"id": head_move_canon},
                    },
                },
            }
            Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({
                "in_flight_cards": {
                    head_move_canon: {
                        "card_name": "Unnamed Card",
                        "payload": copy.deepcopy(head_move_body),
                        "queued_at": 1700000000.0,
                    }
                }
            }))
            server.pending_cards.clear()
            self.assertTrue(server.requeue_in_flight_cards())
            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                self.assertEqual(post_json("/", head_move_body), (200, {"status": "ignored", "reason": "unreconciled_action"}))
                self.assertEqual(
                    post_json(
                        "/",
                        head_move_body,
                        headers={"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json", "X-Delivery-Id": "head-move-del-1"},
                    ),
                    (200, {"status": "ignored", "reason": "unreconciled_action"}),
                )
            self.assertTrue(server.task_queue.empty())

            # 5. Distinct ID-less authors, labels, and delivery keys are not conflated by permanent quarantine.
            def _expire_cooldown():
                st_cool = server.load_sidecar_state()
                st_cool["card_activity"] = {}
                st_cool["recent_triggers"] = []
                self.assertTrue(server.save_sidecar_state(st_cool))

            Path(server.SIDECAR_STATE_FILE).write_text("{}")
            server.pending_cards.clear()
            card_fp = "69aa75000000000000000005"
            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Shared text", "triggerUserUsername": "pm_user"}), (202, {"status": "accepted"}))
                k_fp, _, p_fp = server.task_queue.get_nowait()
                self.assertTrue(server.mark_in_flight_dispatched(k_fp, admission_id=p_fp["_admission_id"]))
                server.pending_cards.clear()
                self.assertTrue(server.requeue_in_flight_cards())
                _expire_cooldown()

                # Same author & same text is blocked; different author & same text is accepted.
                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Shared text", "triggerUserUsername": "pm_user"}), (200, {"status": "ignored", "reason": "unreconciled_action"}))
                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Shared text", "triggerUserUsername": "qa_user"}), (202, {"status": "accepted"}))
                server.release_pending_card(server.task_queue.get_nowait()[0])
                _expire_cooldown()

                # Distinct addLabelToCard labels are separated even though comment is stripped.
                self.assertEqual(post_json("/", {"cardId": card_fp, "actionType": "addLabelToCard", "label": "Bug"}), (202, {"status": "accepted"}))
                k_lbl, _, p_lbl = server.task_queue.get_nowait()
                self.assertTrue(server.mark_in_flight_dispatched(k_lbl, admission_id=p_lbl["_admission_id"]))
                server.pending_cards.clear()
                self.assertTrue(server.requeue_in_flight_cards())
                _expire_cooldown()
                self.assertEqual(post_json("/", {"cardId": card_fp, "actionType": "addLabelToCard", "label": "Bug"}), (200, {"status": "ignored", "reason": "unreconciled_action"}))
                self.assertEqual(post_json("/", {"cardId": card_fp, "actionType": "addLabelToCard", "label": "Urgent"}), (202, {"status": "accepted"}))
                server.release_pending_card(server.task_queue.get_nowait()[0])
                _expire_cooldown()

                # Distinct X-Delivery-Id headers separate identical ID-less payloads, and stable key blocks even when metadata (cardName) changes.
                h_del1 = {"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json", "X-Delivery-Id": "del-1"}
                h_del2 = {"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json", "X-Delivery-Id": "del-2"}
                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Ping"}, headers=h_del1), (202, {"status": "accepted"}))
                k_d1, _, p_d1 = server.task_queue.get_nowait()
                self.assertTrue(server.mark_in_flight_dispatched(k_d1, admission_id=p_d1["_admission_id"]))
                server.pending_cards.clear()
                self.assertTrue(server.requeue_in_flight_cards())
                _expire_cooldown()
                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Ping"}, headers=h_del1), (200, {"status": "ignored", "reason": "unreconciled_action"}))
                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Ping"}, headers=h_del2), (202, {"status": "accepted"}))

                # If del-2 is also interrupted, both share one fingerprint; reconciling by shared fingerprint returns 409 and leaves both blocked until recordId disambiguates.
                k_d2, _, p_d2 = server.task_queue.get_nowait()
                self.assertTrue(server.mark_in_flight_dispatched(k_d2, admission_id=p_d2["_admission_id"]))
                server.pending_cards.clear()
                self.assertTrue(server.requeue_in_flight_cards())
                _expire_cooldown()

                shared_ping_fp = server.relay_replay_fingerprint(card_fp, {"cardId": card_fp, "comment": "Ping"}, "Ping", "")
                with self.assertRaises(urllib.error.HTTPError) as ctx_409:
                    post_json("/admin/reconcile", {"fingerprint": shared_ping_fp})
                self.assertEqual(ctx_409.exception.code, 409)
                body_409 = json.loads(ctx_409.exception.read().decode("utf-8"))
                ctx_409.exception.close()
                self.assertEqual(len(body_409.get("matchingRecordIds", [])), 2)
                self.assertFalse(server.reconcile_quarantined_entry(fingerprint=shared_ping_fp))

                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Ping"}, headers=h_del1), (200, {"status": "ignored", "reason": "unreconciled_action"}))
                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Ping"}, headers=h_del2), (200, {"status": "ignored", "reason": "unreconciled_action"}))

                st_amb = server.load_sidecar_state()
                del1_rec_id = next(
                    k for k, t in st_amb["unreconciled_tombstones"].items()
                    if f"{card_fp}:del-1" in (t.get("delivery_keys") or ())
                )
                del2_rec_id = next(
                    k for k, t in st_amb["unreconciled_tombstones"].items()
                    if f"{card_fp}:del-2" in (t.get("delivery_keys") or ())
                )
                detail_ping_fp = st_amb["unreconciled_cards"][del1_rec_id]["fingerprint"]
                self.assertNotEqual(detail_ping_fp, shared_ping_fp)
                st_amb["unreconciled_cards"].pop(del1_rec_id)
                self.assertTrue(server.save_sidecar_state(st_amb))

                with self.assertRaises(urllib.error.HTTPError) as ctx_cfp_409:
                    post_json("/admin/reconcile", {"fingerprint": detail_ping_fp})
                self.assertEqual(ctx_cfp_409.exception.code, 409)
                body_cfp_409 = json.loads(ctx_cfp_409.exception.read().decode("utf-8"))
                ctx_cfp_409.exception.close()
                self.assertEqual(body_cfp_409.get("matchingRecordIds"), sorted([del1_rec_id, del2_rec_id]))
                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Ping"}, headers=h_del1), (200, {"status": "ignored", "reason": "unreconciled_action"}))
                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Ping"}, headers=h_del2), (200, {"status": "ignored", "reason": "unreconciled_action"}))

                st_d1_clr, bdy_d1_clr = post_json("/admin/reconcile", {"recordId": del1_rec_id})
                self.assertEqual(st_d1_clr, 200)
                self.assertEqual(bdy_d1_clr["cleared"], 1)
                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Ping"}, headers=h_del1), (202, {"status": "accepted"}))
                server.release_pending_card(server.task_queue.get_nowait()[0])
                _expire_cooldown()
                self.assertEqual(post_json("/", {"cardId": card_fp, "comment": "Ping"}, headers=h_del2), (200, {"status": "ignored", "reason": "unreconciled_action"}))
                _expire_cooldown()

                h_meta1 = {"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json", "X-Delivery-Id": "del-meta-1"}
                h_meta2 = {"X-Auth-Token": "secret-webhook-token", "Content-Type": "application/json", "X-Delivery-Id": "del-meta-2"}
                self.assertEqual(post_json("/", {"cardId": card_fp, "cardName": "Old Title", "actionType": "addLabelToCard", "label": "RetryLbl"}, headers=h_meta1), (202, {"status": "accepted"}))
                k_m1, _, p_m1 = server.task_queue.get_nowait()
                self.assertTrue(server.mark_in_flight_dispatched(k_m1, admission_id=p_m1["_admission_id"]))
                server.pending_cards.clear()
                self.assertTrue(server.requeue_in_flight_cards())
                _expire_cooldown()
                self.assertEqual(post_json("/", {"cardId": card_fp, "cardName": "Refreshed Title", "actionType": "addLabelToCard", "label": "RetryLbl"}, headers=h_meta1), (200, {"status": "ignored", "reason": "unreconciled_action"}))
                self.assertEqual(post_json("/", {"cardId": card_fp, "cardName": "Old Title", "actionType": "addLabelToCard", "label": "RetryLbl"}, headers=h_meta2), (202, {"status": "accepted"}))
                server.release_pending_card(server.task_queue.get_nowait()[0])

            # 6. Unified atomic POST /admin/reconcile (authenticated only) clears both actionId & fingerprint
            # even after unreconciled_cards detail pruning, while preserving unrelated records.
            for mode_key in ("actionId", "fingerprint"):
                card_rec = "69aa76000000000000000006"
                card_other = "69aa76000000000000000099"
                body_idless = {"cardId": card_rec, "comment": "Atomic reconcile target", "triggerUserUsername": "pm_user"}
                Path(server.SIDECAR_STATE_FILE).write_text("{}")
                server.pending_cards.clear()
                with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                    self.assertEqual(post_json("/", body_idless), (202, {"status": "accepted"}))
                k_rec, _, p_rec = server.task_queue.get_nowait()
                learned_aid = f"act-atomic-{mode_key}"
                self.assertTrue(server.mark_in_flight_dispatched(k_rec, admission_id=p_rec["_admission_id"], action_id=learned_aid))
                server.pending_cards.clear()
                self.assertTrue(server.requeue_in_flight_cards())

                # Add 505 newer quarantine details (including unrelated card_other) so target's detail row is pruned from unreconciled_cards.
                st_cap = server.load_sidecar_state()
                target_fp = next(iter(st_cap["unreconciled_tombstones"].values()))["fingerprints"][0]
                for i in range(504):
                    st_cap["unreconciled_cards"][f"69cc00{i:018d}:act-pad-{i}"] = {"card_id": f"69cc00{i:018d}", "action_id": f"act-pad-{i}", "stage": "dispatched"}
                st_cap["unreconciled_cards"][f"{card_other}:act-keep-other"] = {"card_id": card_other, "action_id": "act-keep-other", "stage": "dispatched"}
                self.assertTrue(server.save_sidecar_state(st_cap))
                self.assertTrue(server.requeue_in_flight_cards())
                st_pruned = server.load_sidecar_state()
                self.assertEqual(len(st_pruned["unreconciled_cards"]), 500)
                self.assertFalse(any(e.get("action_id") == learned_aid for e in st_pruned["unreconciled_cards"].values()))

                # Unauthenticated and valid-HMAC-only requests to /admin/reconcile must return 401.
                reconcile_check_body = {"actionId": learned_aid}
                raw_reconcile_bytes = json.dumps(reconcile_check_body).encode("utf-8")
                valid_reconcile_hmac = base64.b64encode(
                    hmac.new(
                        b"sec-r7",
                        raw_reconcile_bytes + server.TRELLO_WEBHOOK_CALLBACK_URL.encode("utf-8"),
                        hashlib.sha1,
                    ).digest()
                ).decode("ascii")
                self.assertTrue(
                    server.verify_trello_signature(
                        raw_reconcile_bytes,
                        valid_reconcile_hmac,
                        server.TRELLO_WEBHOOK_CALLBACK_URL,
                        "sec-r7",
                    )
                )
                for bad_hdrs in (
                    {"Content-Type": "application/json"},
                    {"Content-Type": "application/json", "X-Trello-Webhook": valid_reconcile_hmac},
                ):
                    with self.assertRaises(urllib.error.HTTPError) as ctx_401:
                        post_json("/admin/reconcile", reconcile_check_body, headers=bad_hdrs)
                    self.assertEqual(ctx_401.exception.code, 401)
                    ctx_401.exception.close()

                reconcile_body = {"actionId": learned_aid} if mode_key == "actionId" else {"fingerprint": target_fp}
                st_ok, bdy_ok = post_json("/admin/reconcile", reconcile_body)
                self.assertEqual(st_ok, 200)
                self.assertEqual(bdy_ok["cleared"], 1)

                # Both ID-less replay and actionId replay now succeed; unrelated card_other stays blocked.
                with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                    self.assertEqual(post_json("/", body_idless), (202, {"status": "accepted"}))
                    server.release_pending_card(server.task_queue.get_nowait()[0])
                    self.assertEqual(post_json("/", {**body_idless, "action": {"id": learned_aid, "type": "commentCard", "memberCreator": {"username": "pm_user"}}}), (202, {"status": "accepted"}))
                    server.release_pending_card(server.task_queue.get_nowait()[0])
                    self.assertEqual(post_json("/", {"cardId": card_other, "action": {"id": "act-keep-other", "type": "commentCard"}, "comment": "Blocked"}), (200, {"status": "ignored", "reason": "unreconciled_action"}))

            # 6b. Reconciling action X by recordId cannot be replayed to clear a later readmitted run of X.
            Path(server.SIDECAR_STATE_FILE).write_text("{}")
            server.pending_cards.clear()
            card_rec_x = "69aa76b0000000000000006b"
            body_rec_x = {
                "cardId": card_rec_x,
                "action": {"id": "act-rec-replay-x", "type": "commentCard", "memberCreator": {"username": "pm_user"}},
                "comment": "Retry recordId test",
            }
            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                self.assertEqual(post_json("/", body_rec_x), (202, {"status": "accepted"}))
                k_r1, _, p_r1 = server.task_queue.get_nowait()
                self.assertTrue(server.mark_in_flight_dispatched(k_r1, admission_id=p_r1["_admission_id"], action_id="act-rec-replay-x"))
                server.pending_cards.clear()
                self.assertTrue(server.requeue_in_flight_cards())

                st_r1 = server.load_sidecar_state()
                rec_id_1 = next(iter(st_r1["unreconciled_tombstones"].keys()))
                self.assertNotEqual(rec_id_1, f"{card_rec_x}:act-rec-replay-x")
                self.assertIn(p_r1["_admission_id"], rec_id_1)
                st_c1, bdy_c1 = post_json("/admin/reconcile", {"recordId": rec_id_1})
                self.assertEqual(st_c1, 200)
                self.assertEqual(bdy_c1["cleared"], 1)

                # Clear recent_triggers/cooldown so action X can be readmitted cleanly for run 2.
                st_r1_clear = server.load_sidecar_state()
                st_r1_clear["recent_triggers"] = []
                st_r1_clear["card_activity"] = {}
                self.assertTrue(server.save_sidecar_state(st_r1_clear))

                self.assertEqual(post_json("/", body_rec_x), (202, {"status": "accepted"}))
                k_r2, _, p_r2 = server.task_queue.get_nowait()
                self.assertTrue(server.mark_in_flight_dispatched(k_r2, admission_id=p_r2["_admission_id"], action_id="act-rec-replay-x"))
                server.pending_cards.clear()
                self.assertTrue(server.requeue_in_flight_cards())

                st_r2 = server.load_sidecar_state()
                rec_id_2 = next(iter(st_r2["unreconciled_tombstones"].keys()))
                self.assertNotEqual(rec_id_2, rec_id_1)
                st_c2, bdy_c2 = post_json("/admin/reconcile", {"recordId": rec_id_1})
                self.assertEqual(st_c2, 200)
                self.assertEqual(bdy_c2["cleared"], 0)
                self.assertEqual(post_json("/", body_rec_x), (200, {"status": "ignored", "reason": "unreconciled_action"}))

            # 7. Reconciling an older quarantined run does NOT wipe a newer delivery's cooldown on the same card.
            Path(server.SIDECAR_STATE_FILE).write_text("{}")
            server.pending_cards.clear()
            card_own = "69aa77000000000000000007"
            author_a_body = {"cardId": card_own, "comment": "Same cooldown text", "triggerUserUsername": "pm_user"}
            author_b_body = {"cardId": card_own, "comment": "Same cooldown text", "triggerUserUsername": "qa_user"}
            with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                self.assertEqual(post_json("/", author_a_body), (202, {"status": "accepted"}))
                ko, _, po = server.task_queue.get_nowait()
                self.assertTrue(server.mark_in_flight_dispatched(ko, admission_id=po["_admission_id"]))
                server.pending_cards.clear()
                self.assertTrue(server.requeue_in_flight_cards())

                # Expire cooldown (keeping Author A's tombstone), then admit & complete Author B with identical text.
                st_cool_own = server.load_sidecar_state()
                tomb_a_id = next(iter(st_cool_own["unreconciled_tombstones"].keys()))
                st_cool_own["card_activity"][card_own]["last_seen"] = time.time() - (server.TRIGGER_COOLDOWN_SECONDS + 60)
                self.assertTrue(server.save_sidecar_state(st_cool_own))
                self.assertEqual(post_json("/", author_b_body), (202, {"status": "accepted"}))
                server.release_pending_card(server.task_queue.get_nowait()[0])

                # Reconcile Author A by recordId; immediate retry of Author B must still hit trigger_cooldown.
                st_ra, bdy_ra = post_json("/admin/reconcile", {"recordId": tomb_a_id})
                self.assertEqual(st_ra, 200)
                self.assertEqual(bdy_ra["cleared"], 1)
                self.assertEqual(post_json("/", author_b_body), (200, {"status": "ignored", "reason": "trigger_cooldown"}))

            # 8. Pre-unified split-index upgrade with detail already evicted surfaces remainingLegacyOrphans and clears cleanly.
            card_sp = "69aa78000000000000000008"
            sp_idless = {"cardId": card_sp, "comment": "Split orphan text"}
            sp_action = {"cardId": card_sp, "comment": "Split orphan text", "action": {"id": "act-split-orphan", "type": "commentCard"}}
            sp_fp = server.trigger_fingerprint(card_sp, sp_idless, "Split orphan text", "")

            # One-selector reconcile clears legacy-action and reports remaining legacy-fp orphan; reconciling both releases both forms.
            for multi_selector in (False, True):
                Path(server.SIDECAR_STATE_FILE).write_text(json.dumps({
                    "unreconciled_action_ids": {"act-split-orphan": card_sp},
                    "unreconciled_fingerprints": {sp_fp: {"card_id": card_sp, "interrupted_at": 10.0}},
                }))
                server.pending_cards.clear()
                self.assertTrue(server.requeue_in_flight_cards())
                with mock.patch.object(server, "fetch_card_details_sync", return_value=None):
                    if not multi_selector:
                        st_s1, bdy_s1 = post_json("/admin/reconcile", {"actionId": "act-split-orphan"})
                        self.assertEqual(st_s1, 200)
                        self.assertEqual(bdy_s1["cleared"], 1)
                        self.assertTrue(bdy_s1.get("remainingLegacyOrphans"))
                        self.assertTrue(bdy_s1["remainingLegacyOrphans"][0].startswith("legacy-fp:"))
                        self.assertEqual(post_json("/", sp_idless), (200, {"status": "ignored", "reason": "unreconciled_action"}))
                        st_s2, bdy_s2 = post_json("/admin/reconcile", {"recordId": bdy_s1["remainingLegacyOrphans"][0]})
                        self.assertEqual(st_s2, 200)
                        self.assertEqual(bdy_s2["cleared"], 1)
                        self.assertNotIn("remainingLegacyOrphans", bdy_s2)
                    else:
                        st_sb, bdy_sb = post_json("/admin/reconcile", {"actionId": "act-split-orphan", "fingerprint": sp_fp})
                        self.assertEqual(st_sb, 200)
                        self.assertEqual(bdy_sb["cleared"], 2)
                        self.assertNotIn("remainingLegacyOrphans", bdy_sb)
                    self.assertEqual(post_json("/", sp_idless), (202, {"status": "accepted"}))
                    server.release_pending_card(server.task_queue.get_nowait()[0])
                    st_sp_clr = server.load_sidecar_state()
                    st_sp_clr["card_activity"] = {}
                    self.assertTrue(server.save_sidecar_state(st_sp_clr))
                    self.assertEqual(post_json("/", sp_action), (202, {"status": "accepted"}))
                    server.release_pending_card(server.task_queue.get_nowait()[0])
        finally:
            httpd.shutdown()
            httpd.server_close()

    def test_failed_dispatch_checkpoint_preserves_custody_and_exhausts_atomically(self):
        # (a) Transient dispatch checkpoint failures keep queued row + pending_cards and recover via deferred timer.
        server = load_server({
            "TRELLO_DEFERRED_RETRY_MAX_ATTEMPTS": "2",
            "TRELLO_DEFERRED_RETRY_DELAY_SECONDS": "0.05",
        })
        card_t = "69aa81000000000000000001"
        payload_t = {
            "cardId": card_t,
            "comment": "Recover transient checkpoint",
            "action": {"id": "act-trans-1", "type": "commentCard", "memberCreator": {"username": "pm_user"}},
        }
        self.assertTrue(server.should_accept_trigger(card_t, "Transient CP", payload_t, "Recover transient checkpoint", "Ideas")[0])
        orig_save = server.save_sidecar_state
        fail_disp_count = {"n": 0}

        def fail_two_dispatched_saves(state):
            entry = state.get("in_flight_cards", {}).get(card_t, {})
            if entry.get("stage") == "dispatched" and fail_disp_count["n"] < 2:
                fail_disp_count["n"] += 1
                return False
            return orig_save(state)

        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server, "save_sidecar_state", side_effect=fail_two_dispatched_saves),
            mock.patch.object(server.asyncio, "create_subprocess_exec") as mock_exec_t,
        ):
            # Storage is immediately healthy during defer/finally after both checkpoint saves fail.
            server.run_agent_job(card_t, "Transient CP", copy.deepcopy(payload_t))
        mock_exec_t.assert_not_called()
        self.assertEqual(fail_disp_count["n"], 2)
        self.assertIn(card_t, server.pending_cards)
        st_after_fail = server.load_sidecar_state()
        self.assertEqual(st_after_fail["in_flight_cards"][card_t]["stage"], "queued")

        async def fake_trans_ok(*cmd, **kwargs):
            return FakeStreamProcess(
                lines=[
                    json.dumps({"event": "init", "conversation_id": "conv-trans-ok"}),
                    json.dumps({"event": "result", "result": {"conversation_id": "conv-trans-ok", "status": "SUCCESS", "response": "OK", "usage": {"total_tokens": 5}}}),
                ],
                returncode=0,
            )

        q_k, q_n, q_p = server.task_queue.get(timeout=1.5)
        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server.asyncio, "create_subprocess_exec", side_effect=fake_trans_ok),
        ):
            server.run_agent_job(q_k, q_n, q_p)
        self.assertNotIn(card_t, server.pending_cards)
        self.assertNotIn(card_t, server.load_sidecar_state().get("in_flight_cards", {}))

        # (b) Repeated dispatch checkpoint failure reaches exhaustion and clears queued row + dedup in one save.
        card_ex = "69aa82000000000000000002"
        payload_ex = {
            "cardId": card_ex,
            "comment": "Exhaust dispatch checkpoint",
            "action": {"id": "act-trans-ex", "type": "commentCard", "memberCreator": {"username": "pm_user"}},
        }
        self.assertTrue(server.should_accept_trigger(card_ex, "Exhaust CP", payload_ex, "Exhaust dispatch checkpoint", "Ideas")[0])

        def always_fail_dispatched_save(state):
            if state.get("in_flight_cards", {}).get(card_ex, {}).get("stage") == "dispatched":
                return False
            return orig_save(state)

        with (
            mock.patch.object(server, "fetch_card_details_sync", return_value=None),
            mock.patch.object(server, "classify_action", side_effect=mock.AsyncMock(return_value="GENERAL")),
            mock.patch.object(server, "save_sidecar_state", side_effect=always_fail_dispatched_save),
            mock.patch.object(server.asyncio, "create_subprocess_exec") as mock_ex_exec,
        ):
            server.run_agent_job(card_ex, "Exhaust CP", copy.deepcopy(payload_ex))
            k2, n2, p2 = server.task_queue.get(timeout=1.5)
            server.run_agent_job(k2, n2, p2)
            k3, n3, p3 = server.task_queue.get(timeout=1.5)
            server.run_agent_job(k3, n3, p3)
        mock_ex_exec.assert_not_called()
        self.assertNotIn(card_ex, server.pending_cards)
        st_ex_done = server.load_sidecar_state()
        self.assertEqual(st_ex_done.get("in_flight_cards"), {})
        self.assertFalse(any(rt.get("id") == "act-trans-ex" for rt in st_ex_done.get("recent_triggers", [])))
        self.assertEqual(
            server.should_accept_trigger(card_ex, "Exhaust CP", copy.deepcopy(payload_ex), "Exhaust dispatch checkpoint", "Ideas"),
            (True, "accepted"),
        )


if __name__ == "__main__":
    unittest.main()
