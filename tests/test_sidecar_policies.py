import importlib.util
import json
import tempfile
import time
import unittest
from pathlib import Path


SERVER_PATH = (
    Path(__file__).resolve().parents[1]
    / ".agents/plugins/trello-integration/sidecars/trello-webhook-receiver/server.py"
)
HELPER_PATH = (
    Path(__file__).resolve().parents[1]
    / ".agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py"
)


def load_server():
    spec = importlib.util.spec_from_file_location("trello_sidecar_server", SERVER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    tempdir = Path(tempfile.mkdtemp())
    module.SIDECAR_STATE_FILE = str(tempdir / "sidecar_state.json")
    module.SIDECAR_EVENT_LOG_FILE = str(tempdir / "sidecar_events.jsonl")
    module.pending_cards.clear()
    return module


def load_helper():
    spec = importlib.util.spec_from_file_location("trello_helper", HELPER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


if __name__ == "__main__":
    unittest.main()
