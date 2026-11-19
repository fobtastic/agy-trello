import asyncio
import base64
from collections import deque
import concurrent.futures
import copy
import hashlib
import hmac
import json
import logging
import os
import queue
import re
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Load environment variables from ~/.zshrc (especially TRELLO_API_KEY, TRELLO_SECRET, TRELLO_API_TOKEN)
def load_zshrc_env():
    zshrc_path = os.path.expanduser("~/.zshrc")
    if os.path.exists(zshrc_path):
        import re
        try:
            with open(zshrc_path, "r") as f:
                content = f.read()
                for key in ["TRELLO_API_KEY", "TRELLO_SECRET", "TRELLO_API_TOKEN", "TRELLO_TOKEN"]:
                    if key not in os.environ:
                        match = re.search(fr'export {key}=(.*)', content)
                        if match:
                            value = match.group(1).strip().strip('"').strip("'")
                            os.environ[key] = value
                            logging.info(f"[Trello Sidecar] Injected {key} from ~/.zshrc")
        except Exception as e:
            logging.error(f"[Trello Sidecar] Error loading environment from ~/.zshrc: {e}")

load_zshrc_env()

def load_dotenv():
    # Look for .env in the script's directory and current working directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(script_dir, ".env"),
        os.path.join(os.getcwd(), ".env")
    ]
    import re
    for dotenv_path in candidates:
        if os.path.exists(dotenv_path):
            try:
                with open(dotenv_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        # Ignore comments and empty lines
                        if not line or line.startswith("#"):
                            continue
                        match = re.match(r'^(?:export\s+)?([\w_]+)\s*=\s*(.*)$', line)
                        if match:
                            key = match.group(1)
                            value = match.group(2).strip().strip('"').strip("'")
                            os.environ[key] = value
                            logging.info(f"[Trello Sidecar] Loaded/Overwrote {key} from {dotenv_path}")
            except Exception as e:
                logging.error(f"[Trello Sidecar] Error parsing dotenv file {dotenv_path}: {e}")

load_dotenv()

def _collect_env_secrets():
    values = set()
    for key, secret_value in os.environ.items():
        if secret_value and len(secret_value) >= 6 and re.search(r'(KEY|TOKEN|SECRET|PASSWORD)', key, re.I):
            values.add(secret_value)
    return values

INITIAL_ENV_SECRETS = _collect_env_secrets()

def redact_sensitive(value):
    text = str(value)
    text = re.sub(r'([?&](?:key|token)=)[^&\s]+', r'\1[REDACTED]', text, flags=re.IGNORECASE)
    text = re.sub(r'(Bearer\s+)[A-Za-z0-9._~+/-]+=*', r'\1[REDACTED]', text, flags=re.IGNORECASE)
    text = re.sub(r'\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]+\b', '[REDACTED]', text)
    text = re.sub(r'\bgithub_pat_[A-Za-z0-9_]+\b', '[REDACTED]', text)
    secrets_to_mask = INITIAL_ENV_SECRETS | _collect_env_secrets()
    for secret_value in sorted(secrets_to_mask, key=len, reverse=True):
        text = text.replace(secret_value, "[REDACTED]")
    return text

def trello_path_id(value):
    return urllib.parse.quote(str(value or "").strip(), safe="")

def fetch_card_details_sync(card_id):
    api_key = os.environ.get("TRELLO_API_KEY")
    api_token = os.environ.get("TRELLO_API_TOKEN") or os.environ.get("TRELLO_TOKEN")
    if not api_key or not api_token:
        logging.warning("[Trello Sidecar] Missing Trello API key or token, cannot fetch full card details.")
        return None
    
    # Query parameters to fetch card, list, actions, checklists, and members in one go
    params = {
        "key": api_key,
        "token": api_token,
        "actions": "commentCard",
        "checklists": "all",
        "list": "true",
        "members": "true",
        "fields": "id,shortLink,name,desc,closed,idList,url,labels,due,dateLastActivity"
    }
    url = f"https://api.trello.com/1/cards/{trello_path_id(card_id)}?" + urllib.parse.urlencode(params)
    
    logging.info(f"[Trello Sidecar] Fetching card details from Trello for card ID: {card_id}...")
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status == 200:
                return json.loads(response.read().decode('utf-8'))
            else:
                logging.error(f"[Trello Sidecar] Failed to fetch card details. HTTP Status: {response.status}")
    except Exception as e:
        logging.error(f"[Trello Sidecar] Exception fetching card details: {type(e).__name__}: {redact_sensitive(e)}")
    return None

def _invoke_outbound_urlopen(req, timeout=10):
    with process_cond:
        if outbound_posts_closed_event.is_set() or shutdown_event.is_set():
            return None
    return urllib.request.urlopen(req, timeout=timeout)

def _open_outbound_post_request(req, timeout=10):
    with _outbound_post_gate_lock:
        if outbound_posts_closed_event.is_set() or shutdown_event.is_set():
            return None
        return _invoke_outbound_urlopen(req, timeout=timeout)

def post_trello_comment(card_id, text, kind="comment"):
    api_key = os.environ.get("TRELLO_API_KEY")
    api_token = os.environ.get("TRELLO_API_TOKEN") or os.environ.get("TRELLO_TOKEN")
    if not api_key or not api_token or not card_id:
        logging.error("[Trello Sidecar] Cannot post %s comment: missing credentials or card_id.", kind)
        return False
    url = f"https://api.trello.com/1/cards/{trello_path_id(card_id)}/actions/comments?key={api_key}&token={api_token}"
    data = {"text": text}
    encoded_data = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, method="POST", data=encoded_data)
    req.add_header("Accept", "application/json")
    if TRELLO_CLIENT_IDENTIFIER:
        req.add_header("X-Trello-Client-Identifier", TRELLO_CLIENT_IDENTIFIER)
    if not begin_outbound_post():
        logging.info("[Trello Sidecar] Skipping %s Trello comment due to server shutdown.", kind)
        return False
    try:
        resp = _open_outbound_post_request(req, timeout=10)
        if resp is None:
            logging.info("[Trello Sidecar] Skipping %s Trello comment due to server shutdown.", kind)
            return False
        with resp:
            status = getattr(resp, "status", 200)
            if 200 <= status < 300:
                logging.info(f"[Trello Sidecar] Posted {kind} comment on Trello card {card_id}")
                return True
            logging.error(f"[Trello Sidecar] Failed to post {kind} comment: HTTP {status}")
            return False
    except Exception as e:
        logging.error(f"[Trello Sidecar] Failed to post {kind} comment: {type(e).__name__}: {redact_sensitive(e)}")
        return False
    finally:
        finish_outbound_post()

def format_card_details(card_data):
    if not card_data:
        return "No card details could be fetched."
    
    name = card_data.get("name", "Unnamed Card")
    desc = card_data.get("desc", "")
    closed = "Closed" if card_data.get("closed") else "Active"
    
    list_info = card_data.get("list", {})
    list_name = list_info.get("name", "Unknown List")
    
    labels = card_data.get("labels", [])
    label_names = ", ".join([label.get("name") or label.get("color") for label in labels]) if labels else "None"
    
    due = card_data.get("due", "None")
    last_activity = card_data.get("dateLastActivity", "Unknown")
    url = card_data.get("url", "")
    
    # Checklists
    checklists = card_data.get("checklists", [])
    checklists_str = ""
    if checklists:
        for cl in checklists:
            checklists_str += f"- Checklist: {cl.get('name')}\n"
            for item in cl.get("checkItems", []):
                state_char = "x" if item.get("state") == "complete" else " "
                checklists_str += f"  - [{state_char}] {item.get('name')}\n"
    else:
        checklists_str = "None\n"
        
    # Comments (actions)
    actions = card_data.get("actions", [])
    comments_str = ""
    # Sort comments chronologically (oldest first)
    comments = []
    for action in actions:
        if action.get("type") == "commentCard":
            member = action.get("memberCreator", {})
            user = member.get("fullName") or member.get("username") or "Unknown User"
            date = action.get("date", "")
            text = action.get("data", {}).get("text", "")
            comments.append((date, user, text))
            
    # Sort by date ascending
    comments.sort(key=lambda x: x[0])
    
    if comments:
        if len(comments) > MAX_CARD_COMMENTS_IN_PROMPT:
            omitted = len(comments) - MAX_CARD_COMMENTS_IN_PROMPT
            comments = comments[-MAX_CARD_COMMENTS_IN_PROMPT:]
            comments_str += f"- ... ({omitted} earlier comments omitted)\n"
        for date, user, text in comments:
            comments_str += f"- **{user}** ({date}): {text}\n"
    else:
        comments_str = "None\n"
        
    formatted = (
        f"### Card: {name}\n"
        f"- **Status**: {closed}\n"
        f"- **List**: {list_name}\n"
        f"- **Labels**: {label_names}\n"
        f"- **Due Date**: {due}\n"
        f"- **Last Activity**: {last_activity}\n"
        f"- **URL**: {url}\n\n"
        f"### Description:\n"
        f"{desc}\n\n"
        f"### Checklists:\n"
        f"{checklists_str}\n"
        f"### Comments/Discussion History:\n"
        f"{comments_str}"
    )
    return formatted

# 5. Agent signature name configuration
def get_env_int(name, default):
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logging.warning("[Trello Sidecar] Invalid integer for %s=%r. Using default %s.", name, raw, default)
        return default

DEFAULT_AGENT_MODEL = os.environ.get("TRELLO_AGENT_MODEL", "gemini-pro-orchestrator").strip() or "gemini-pro-orchestrator"
CLASSIFIER_MODEL = os.environ.get("TRELLO_CLASSIFIER_MODEL", DEFAULT_AGENT_MODEL).strip() or DEFAULT_AGENT_MODEL
CODEX_REVIEW_MODEL = os.environ.get("TRELLO_CODEX_REVIEW_MODEL", "gpt-6.2-sol").strip() or "gpt-6.2-sol"
AGENT_PRINT_TIMEOUT = os.environ.get("TRELLO_AGENT_PRINT_TIMEOUT", "15m").strip() or "15m"
CLASSIFIER_PRINT_TIMEOUT = os.environ.get("TRELLO_CLASSIFIER_PRINT_TIMEOUT", "2m").strip() or "2m"
AGENT_SUBPROCESS_TIMEOUT_SECONDS = get_env_int("TRELLO_AGENT_TIMEOUT_SECONDS", 960)
CLASSIFIER_TIMEOUT_SECONDS = get_env_int("TRELLO_CLASSIFIER_TIMEOUT_SECONDS", 150)
MAX_CARD_COMMENTS_IN_PROMPT = get_env_int("TRELLO_MAX_CARD_COMMENTS", 30)
MIN_PROMPT_BYTES = 16384
MIN_CLASSIFIER_PROMPT_BYTES = 2048
_configured_max_prompt_bytes = get_env_int("TRELLO_MAX_PROMPT_BYTES", 96000)
if _configured_max_prompt_bytes < MIN_PROMPT_BYTES:
    logging.warning(
        "[Trello Sidecar] TRELLO_MAX_PROMPT_BYTES=%s below minimum %s; clamping to %s.",
        _configured_max_prompt_bytes,
        MIN_PROMPT_BYTES,
        MIN_PROMPT_BYTES,
    )
MAX_PROMPT_BYTES = max(MIN_PROMPT_BYTES, _configured_max_prompt_bytes)
DEFERRED_RETRY_MAX_ATTEMPTS = max(0, get_env_int("TRELLO_DEFERRED_RETRY_MAX_ATTEMPTS", 3))
try:
    DEFERRED_RETRY_DELAY_SECONDS = max(
        0.01,
        float(os.environ.get("TRELLO_DEFERRED_RETRY_DELAY_SECONDS", "15")),
    )
except ValueError:
    logging.warning("[Trello Sidecar] Invalid float for TRELLO_DEFERRED_RETRY_DELAY_SECONDS; using default 15.0.")
    DEFERRED_RETRY_DELAY_SECONDS = 15.0
MAX_WEBHOOK_BODY_BYTES = get_env_int("TRELLO_MAX_WEBHOOK_BODY_BYTES", 1024 * 1024)
WEBHOOK_READ_TIMEOUT_SECONDS = float(os.environ.get("TRELLO_WEBHOOK_READ_TIMEOUT_SECONDS", "10"))
MAX_HTTP_WORKERS = get_env_int("TRELLO_MAX_HTTP_WORKERS", 16)
MAX_STREAM_LINE_BYTES = get_env_int("TRELLO_MAX_STREAM_LINE_BYTES", 4 * 1024 * 1024)
TRELLO_HELPER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trello_helper.py")
TRELLO_CLIENT_IDENTIFIER = os.environ.get("TRELLO_CLIENT_IDENTIFIER", "agy-trello-sidecar").strip()
TRELLO_WEBHOOK_CALLBACK_URL = os.environ.get("TRELLO_WEBHOOK_CALLBACK_URL", "").strip()
TRELLO_SECRET = os.environ.get("TRELLO_SECRET", "")
RAW_ACTIONABLE_TYPES = {
    "commentCard",
    "updateCard",
    "createCard",
    "addLabelToCard",
}
NON_COMMENT_ACTION_TYPES = {
    "updateCard",
    "createCard",
    "addLabelToCard",
}
ENABLE_TAILSCALE_FUNNEL = os.environ.get("TRELLO_ENABLE_TAILSCALE_FUNNEL", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
VALID_PHASES = {"READY_FOR_SPEC", "INVESTIGATOR", "GENERAL"}
PHASE_BLOCKED_TOOL_ATTEMPT = "BLOCKED_TOOL_ATTEMPT"
CONVERSATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{3,127}$")
CANONICAL_TRELLO_ID_RE = re.compile(r"^[0-9a-fA-F]{24}$")

def _safe_str(value):
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None

def normalize_canonical_trello_id(value):
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not CANONICAL_TRELLO_ID_RE.fullmatch(stripped):
        return None
    return stripped.lower()

def is_canonical_trello_id(value):
    return normalize_canonical_trello_id(value) is not None

def validate_conversation_id(value):
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped or not CONVERSATION_ID_RE.fullmatch(stripped):
        return None
    return stripped

AGENT_SIGNATURE_NAME = os.environ.get("TRELLO_AGENT_SIGNATURE_NAME", "Agy")
AGENT_TRELLO_USERNAME = os.environ.get("TRELLO_AGENT_TRELLO_USERNAME", "").strip().lower()
SUPPRESSED_TRELLO_USERNAMES = {
    username.strip().lower()
    for username in os.environ.get(
        "TRELLO_SUPPRESSED_TRIGGER_USERNAMES",
        "trello,butler",
    ).split(",")
    if username.strip()
}
SUPPRESSED_COMMENT_REGEX = os.environ.get(
    "TRELLO_SUPPRESSED_COMMENT_REGEX",
    rf"(?im)^\s*-\s*Love\s+{re.escape(AGENT_SIGNATURE_NAME)}\s*$|^\s*-\s*Love\s+(INVESTIGATOR|PLANNER)\s*$",
)
POST_ACK_COMMENTS = os.environ.get("TRELLO_POST_ACK_COMMENTS", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
TRIGGER_COOLDOWN_SECONDS = get_env_int("TRELLO_TRIGGER_COOLDOWN_SECONDS", 300)
MAX_RECENT_TRIGGERS = get_env_int("TRELLO_MAX_RECENT_TRIGGER_IDS", 500)
ACK_COOLDOWN_SECONDS = get_env_int("TRELLO_ACK_COOLDOWN_SECONDS", 900)
AGENT_COMMENT_MARKER = os.environ.get("TRELLO_AGENT_COMMENT_MARKER", "<!-- [agy-sidecar:comment] -->")
AGENT_ACK_MARKER = os.environ.get("TRELLO_AGENT_ACK_MARKER", "<!-- [agy-sidecar:ack] -->")
AGENT_STATUS_MARKER = os.environ.get("TRELLO_AGENT_STATUS_MARKER", "<!-- [agy-sidecar:status] -->")
AGENT_COMMENT_USER_REGEX = os.environ.get(
    "TRELLO_AGENT_COMMENT_USER_REGEX",
    r"(?i)(^|[-_\s])(agent|agy|bot)([-_\s]|$)|^(agent|agy|bot)([-_\s]|$)",
)
LOW_NOVELTY_UNIQUE_TOKEN_RATIO = float(os.environ.get("TRELLO_LOW_NOVELTY_UNIQUE_TOKEN_RATIO", "0.35"))
LOW_NOVELTY_OVERLAP_RATIO = float(os.environ.get("TRELLO_LOW_NOVELTY_OVERLAP_RATIO", "0.72"))

DEFAULT_STAKEHOLDER_CONTEXT_FILE = os.path.expanduser("~/.gemini/antigravity-cli/trello_stakeholders.json")
STAKEHOLDER_CONTEXT_FILE = os.environ.get("TRELLO_STAKEHOLDER_CONTEXT_FILE", DEFAULT_STAKEHOLDER_CONTEXT_FILE)

def normalize_roster_key(value):
    return str(value or "").strip().lower().lstrip("@")

def safe_roster_text(value, max_len=240):
    text = re.sub(r"\s+", " ", str(value or "").strip())
    return text[:max_len]

def load_stakeholder_context():
    raw_context = os.environ.get("TRELLO_STAKEHOLDER_CONTEXT_JSON")
    source = "TRELLO_STAKEHOLDER_CONTEXT_JSON"
    if not raw_context and STAKEHOLDER_CONTEXT_FILE and os.path.exists(os.path.expanduser(STAKEHOLDER_CONTEXT_FILE)):
        source = os.path.expanduser(STAKEHOLDER_CONTEXT_FILE)
        try:
            with open(source, "r") as f:
                raw_context = f.read()
        except Exception as e:
            logging.warning("[Trello Sidecar] Failed to read stakeholder context file %s: %s", source, redact_sensitive(e))
            raw_context = None
    if not raw_context:
        return {"source": None, "users": [], "users_by_username": {}, "rules": []}
    try:
        parsed = json.loads(raw_context)
    except Exception as e:
        logging.warning("[Trello Sidecar] Failed to parse stakeholder context from %s: %s", source, redact_sensitive(e))
        return {"source": None, "users": [], "users_by_username": {}, "rules": []}

    users = parsed.get("users", []) if isinstance(parsed, dict) else []
    rules = parsed.get("rules", []) if isinstance(parsed, dict) else []
    sanitized_users = []
    users_by_username = {}
    allowed_fields = [
        "trello_username",
        "display_name",
        "role",
        "authority",
        "preferred_address",
        "mention_policy",
        "tone",
        "notes",
    ]
    for user in users[:100]:
        if not isinstance(user, dict):
            continue
        sanitized = {
            field: safe_roster_text(user.get(field))
            for field in allowed_fields
            if safe_roster_text(user.get(field))
        }
        username = normalize_roster_key(sanitized.get("trello_username"))
        if not username and not sanitized.get("display_name"):
            continue
        if username:
            sanitized["trello_username"] = username
            users_by_username[username] = sanitized
        sanitized_users.append(sanitized)

    sanitized_rules = [safe_roster_text(rule, 320) for rule in rules[:30] if safe_roster_text(rule, 320)]
    return {
        "source": source,
        "users": sanitized_users,
        "users_by_username": users_by_username,
        "rules": sanitized_rules,
    }

STAKEHOLDER_CONTEXT = load_stakeholder_context()

def resolve_agy_bin():
    agy_bin = shutil.which("agy")
    if agy_bin:
        return agy_bin
    local_agy = os.path.expanduser("~/.local/bin/agy")
    if os.path.exists(local_agy):
        return local_agy
    return "/home/ubuntu/.local/bin/agy"

def format_command_for_log(cmd):
    safe_cmd = []
    skip_next = False
    for part in cmd:
        if skip_next:
            safe_cmd.append("[prompt omitted]")
            skip_next = False
            continue
        safe_cmd.append(part)
        if part == "--print":
            skip_next = True
    return " ".join(safe_cmd)

# 1. Secure Token Resolution
def get_auth_token():
    token = os.environ.get("TRELLO_WEBHOOK_TOKEN")
    if not token:
        # TODO(security): Ensure TRELLO_WEBHOOK_TOKEN is set in the environment for production.
        # Fallback to a secure randomly generated value for testing/sandboxes
        logging.warning("TRELLO_WEBHOOK_TOKEN environment variable not set. Generating ephemeral secret. Instance-isolated!")
        return secrets.token_hex(32)
    return token

AUTH_TOKEN = get_auth_token()

# 2. Resolve target workspaces from environment (comma-separated paths)
WORKSPACES_ENV = os.environ.get("TRELLO_AGENT_WORKSPACES")
WORKSPACES = [w.strip() for w in WORKSPACES_ENV.split(",") if w.strip()] if WORKSPACES_ENV else None

if WORKSPACES:
    logging.info(f"[Trello Sidecar] Pinning target workspaces to: {WORKSPACES}")

def resolve_target_cwd(card_name, card_desc, list_name, workspaces):
    if not workspaces:
        return None
    if len(workspaces) == 1:
        return workspaces[0]
    
    content_lower = f"{card_name} {card_desc} {list_name}".lower()
    
    backend_indicators = [
        "backend", " be ", "[backend]", "api", "db", "database", 
        "laravel", "artisan", "migration", "queue", "php", "query", 
        "server", "worker", "scheduled"
    ]
    frontend_indicators = [
        "frontend", " fe ", "[frontend]", "ui", "design", "css", 
        "styling", "component", "browser", "click", "page", "react", 
        "next", "playwright", "scroll", "selector", "modal", "preview", 
        "zoom", "pixel", "color", "layout", "mobile"
    ]
    
    be_count = sum(1 for ind in backend_indicators if ind in content_lower)
    fe_count = sum(1 for ind in frontend_indicators if ind in content_lower)
    
    fe_ws = next((w for w in workspaces if "frontend" in w.lower() or "fe" in w.lower()), None)
    be_ws = next((w for w in workspaces if "backend" in w.lower() or "be" in w.lower()), None)
    
    # Check if both are requested
    if fe_count > 0 and be_count > 0 and fe_ws and be_ws:
        parent_dir = os.path.dirname(fe_ws)
        logging.info(f"[Trello Sidecar] Card requires BOTH frontend and backend (fe={fe_count}, be={be_count}), resolving to common parent: {parent_dir}")
        return parent_dir
    
    if fe_count > be_count and fe_ws:
        logging.info(f"[Trello Sidecar] Card resolved to Frontend workspace based on keywords (fe={fe_count}, be={be_count}): {fe_ws}")
        return fe_ws
    elif be_count > fe_count and be_ws:
        logging.info(f"[Trello Sidecar] Card resolved to Backend workspace based on keywords (fe={fe_count}, be={be_count}): {be_ws}")
        return be_ws
        
    logging.info(f"[Trello Sidecar] Keywords balanced or workspace mismatch (fe={fe_count}, be={be_count}), fallback to: {workspaces[0]}")
    return workspaces[0]


# 3. Active Process Tracking for Graceful Cleanup
active_processes = {}
process_lock = threading.Lock()
process_cond = threading.Condition(process_lock)
_spawn_gate_lock = threading.RLock()
_outbound_post_gate_lock = threading.RLock()
launches_in_progress = 0
outbound_posts_in_progress = 0
shutdown_event = threading.Event()
outbound_posts_closed_event = threading.Event()

class ShutdownRequested(RuntimeError):
    pass

class DeferredTriggerRetry(Exception):
    def __init__(self, delay_seconds, message="Deferred trigger pending verification", defer_attempts=None):
        super().__init__(message)
        self.delay_seconds = delay_seconds
        self.defer_attempts = defer_attempts

class _DispatchStateError(RuntimeError):
    pass

def _publish_shutdown_intent():
    with process_cond:
        shutdown_event.set()
        process_cond.notify_all()

def _drain_spawn_gate(timeout=3.0):
    acquired = _spawn_gate_lock.acquire(timeout=timeout)
    try:
        with process_cond:
            shutdown_event.set()
            process_cond.notify_all()
    finally:
        if acquired:
            _spawn_gate_lock.release()

def _close_outbound_post_gate(timeout=3.0):
    acquired = _outbound_post_gate_lock.acquire(timeout=timeout)
    try:
        outbound_posts_closed_event.set()
        with process_cond:
            shutdown_event.set()
            process_cond.notify_all()
        return acquired
    finally:
        if acquired:
            _outbound_post_gate_lock.release()

def initiate_shutdown(timeout=3.0):
    _publish_shutdown_intent()
    _drain_spawn_gate(timeout=timeout)
    _close_outbound_post_gate(timeout=timeout)

def _valid_owned_pgid(pgid):
    if isinstance(pgid, bool) or not isinstance(pgid, int):
        return None
    if pgid <= 0 or pgid == os.getpid():
        return None
    return pgid

def begin_process_launch():
    global launches_in_progress
    with process_cond:
        if shutdown_event.is_set():
            return False
        launches_in_progress += 1
        return True

def finish_process_launch(proc):
    global launches_in_progress
    with process_cond:
        launches_in_progress = max(0, launches_in_progress - 1)
        if proc is not None:
            active_processes[proc] = _valid_owned_pgid(getattr(proc, "pid", None))
        process_cond.notify_all()
        return not shutdown_event.is_set()

def begin_outbound_post():
    global outbound_posts_in_progress
    with process_cond:
        if outbound_posts_closed_event.is_set() or shutdown_event.is_set():
            return False
        outbound_posts_in_progress += 1
        return True

def finish_outbound_post():
    global outbound_posts_in_progress
    with process_cond:
        outbound_posts_in_progress = max(0, outbound_posts_in_progress - 1)
        process_cond.notify_all()

def register_active_process_or_abort(proc):
    pgid = _valid_owned_pgid(getattr(proc, "pid", None))
    with process_cond:
        if shutdown_event.is_set():
            return False
        active_processes[proc] = pgid
        return True

def register_active_process(proc):
    return register_active_process_or_abort(proc)

def unregister_active_process(proc):
    with process_cond:
        active_processes.pop(proc, None)
        process_cond.notify_all()

def _get_owned_pgid(proc):
    with process_cond:
        pgid = active_processes.get(proc)
    if pgid is not None:
        return _valid_owned_pgid(pgid)
    return _valid_owned_pgid(getattr(proc, "pid", None))

def _kill_process_group(pid, sig):
    pgid = _valid_owned_pgid(pid)
    if pgid is None:
        return False
    if hasattr(os, "killpg"):
        try:
            os.killpg(pgid, sig)
            return True
        except (ProcessLookupError, PermissionError, OSError):
            return False
    return False

def _process_group_exists(pid):
    pgid = _valid_owned_pgid(pid)
    if pgid is None or not hasattr(os, "killpg"):
        return False
    try:
        os.killpg(pgid, 0)
        return True
    except PermissionError:
        return True
    except (ProcessLookupError, OSError):
        return False

async def terminate_process(process):
    if not process:
        return
    pgid = _get_owned_pgid(process)
    group_alive = _process_group_exists(pgid)
    if process.returncode is not None and not group_alive:
        return
    try:
        group_signaled = _kill_process_group(pgid, signal.SIGTERM) if pgid is not None else False
        if process.returncode is None and not group_signaled:
            process.terminate()

        deadline = time.monotonic() + 3.0
        if process.returncode is None:
            remaining = max(0.05, deadline - time.monotonic())
            try:
                await asyncio.wait_for(process.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                pass
        while pgid is not None and group_signaled and _process_group_exists(pgid) and time.monotonic() < deadline:
            await asyncio.sleep(0.05)

        still_running = (process.returncode is None) or (pgid is not None and _process_group_exists(pgid))
        if still_running:
            kill_signaled = _kill_process_group(pgid, signal.SIGKILL) if pgid is not None else False
            if process.returncode is None and not kill_signaled:
                process.kill()
            kill_deadline = time.monotonic() + 2.0
            if process.returncode is None:
                try:
                    await asyncio.wait_for(process.wait(), timeout=max(0.05, kill_deadline - time.monotonic()))
                except asyncio.TimeoutError:
                    pass
            while pgid is not None and kill_signaled and _process_group_exists(pgid) and time.monotonic() < kill_deadline:
                await asyncio.sleep(0.05)
    except ProcessLookupError:
        pass
    except Exception:
        pass

def _signal_procs_sigterm(procs, signaled_ids, live_groups, live_procs):
    for proc, pgid in procs:
        if id(proc) in signaled_ids:
            continue
        signaled_ids.add(id(proc))
        try:
            pid = _valid_owned_pgid(pgid) or _valid_owned_pgid(getattr(proc, "pid", None))
            if pid is not None and (proc.returncode is None or _process_group_exists(pid)):
                logging.info(f"[Trello Sidecar] Terminating agent process group {pid}...")
                if _kill_process_group(pid, signal.SIGTERM):
                    if pid not in live_groups:
                        live_groups.append(pid)
                elif proc.returncode is None:
                    proc.terminate()
                    live_procs.append(proc)
            elif proc.returncode is None:
                logging.info("[Trello Sidecar] Terminating agent process...")
                proc.terminate()
                live_procs.append(proc)
        except ProcessLookupError:
            pass
        except Exception as e:
            logging.warning(f"[Trello Sidecar] Failed to terminate process {getattr(proc, 'pid', '?')}: {type(e).__name__}")

def cleanup_processes():
    signaled_ids = set()
    live_groups = []
    live_procs = []

    _publish_shutdown_intent()

    with process_cond:
        initial_procs = list(active_processes.items())
    if initial_procs:
        logging.info(f"[Trello Sidecar] Cleaning up {len(initial_procs)} active agent processes...")
        _signal_procs_sigterm(initial_procs, signaled_ids, live_groups, live_procs)

    _drain_spawn_gate(timeout=3.0)

    with process_cond:
        launch_deadline = time.monotonic() + 3.0
        while launches_in_progress > 0:
            remaining = launch_deadline - time.monotonic()
            if remaining <= 0:
                break
            process_cond.wait(timeout=remaining)
        late_procs = list(active_processes.items())
    _signal_procs_sigterm(late_procs, signaled_ids, live_groups, live_procs)

    if live_groups or live_procs:
        deadline = time.monotonic() + 2.0
        while (live_groups or live_procs) and time.monotonic() < deadline:
            live_groups = [pid for pid in live_groups if _process_group_exists(pid)]
            live_procs = [p for p in live_procs if p.returncode is None]
            if not live_groups and not live_procs:
                break
            time.sleep(0.05)

        for pid in live_groups:
            if _process_group_exists(pid):
                _kill_process_group(pid, signal.SIGKILL)
        for proc in live_procs:
            if proc.returncode is None:
                try:
                    proc.kill()
                except Exception:
                    pass

    post_deadline = time.monotonic() + 3.0
    _close_outbound_post_gate(timeout=3.0)

    with process_cond:
        while outbound_posts_in_progress > 0:
            remaining = post_deadline - time.monotonic()
            if remaining <= 0:
                break
            process_cond.wait(timeout=remaining)

# 4. Session/Conversation Tracking (Resuming specific card conversations)
SESSION_MAP_FILE = os.path.expanduser("~/.gemini/antigravity-cli/trello_sessions.json")
SIDECAR_STATE_FILE = os.path.expanduser("~/.gemini/antigravity-cli/trello_sidecar_state.json")
SIDECAR_EVENT_LOG_FILE = os.environ.get(
    "TRELLO_SIDECAR_EVENT_LOG_FILE",
    os.path.expanduser("~/.gemini/antigravity-cli/trello_sidecar_events.jsonl"),
)
state_lock = threading.Lock()
pending_cards = set()

def atomic_write_json(path, data):
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", suffix=".json", dir=parent)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        raise

class SessionMappingReadError(RuntimeError):
    pass

def load_session_mapping_strict():
    try:
        with open(SESSION_MAP_FILE, 'r') as f:
            parsed = json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        logging.error(f"[Trello Sidecar] Error loading session mapping: {type(e).__name__}")
        raise SessionMappingReadError(type(e).__name__) from e
    if not isinstance(parsed, dict):
        logging.error("[Trello Sidecar] Invalid session mapping format: expected object")
        raise SessionMappingReadError("InvalidSessionMapFormat")
    return parsed

def load_session_mapping():
    try:
        return load_session_mapping_strict()
    except SessionMappingReadError:
        return {}

def save_session_mapping(mapping):
    try:
        atomic_write_json(SESSION_MAP_FILE, mapping)
        return True
    except Exception as e:
        logging.error(f"[Trello Sidecar] Error saving session mapping: {type(e).__name__}")
        return False

class SidecarStateReadError(RuntimeError):
    pass

def load_sidecar_state_strict():
    try:
        with open(SIDECAR_STATE_FILE, 'r') as f:
            parsed = json.load(f)
    except FileNotFoundError:
        return {"recent_triggers": [], "card_activity": {}}
    except Exception as e:
        logging.error(f"[Trello Sidecar] Error loading sidecar state: {type(e).__name__}")
        raise SidecarStateReadError(type(e).__name__) from e
    if not isinstance(parsed, dict):
        logging.error("[Trello Sidecar] Invalid sidecar state format: expected object")
        raise SidecarStateReadError("InvalidStateFormat")
    return parsed

def load_sidecar_state():
    try:
        return load_sidecar_state_strict()
    except SidecarStateReadError:
        return {"recent_triggers": [], "card_activity": {}}

def save_sidecar_state(state):
    try:
        atomic_write_json(SIDECAR_STATE_FILE, state)
        return True
    except Exception as e:
        logging.error(f"[Trello Sidecar] Error saving sidecar state: {type(e).__name__}")
        return False

def sanitize_event_value(value):
    if isinstance(value, str):
        return redact_sensitive(value)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [sanitize_event_value(item) for item in value[:20]]
    if isinstance(value, dict):
        return {
            str(key): sanitize_event_value(item)
            for key, item in value.items()
            if str(key).lower() not in {"comment", "comments", "payload", "body", "text", "stdout", "stderr"}
        }
    return redact_sensitive(value)

def log_sidecar_event(event_name, details=None):
    event = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "event": str(event_name),
    }
    for key, value in (details or {}).items():
        key = str(key)
        if key.lower() in {"comment", "comments", "payload", "body", "text", "stdout", "stderr"}:
            continue
        event[key] = sanitize_event_value(value)
    try:
        os.makedirs(os.path.dirname(SIDECAR_EVENT_LOG_FILE), exist_ok=True)
        with open(SIDECAR_EVENT_LOG_FILE, "a") as f:
            f.write(json.dumps(event, sort_keys=True) + "\n")
    except Exception as e:
        logging.warning("[Trello Sidecar] Failed to write sidecar event log: %s", type(e).__name__)

SAFETY_BLOCK_PREFIX = "This request was blocked by Gemini's filters"
CLASSIFIER_ENV_ALLOWLIST = (
    "PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "LC_CTYPE",
    "TMPDIR", "TEMP", "TMP", "SSL_CERT_FILE", "SSL_CERT_DIR",
    "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy",
    "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME",
)

def build_classifier_env():
    return {key: os.environ[key] for key in CLASSIFIER_ENV_ALLOWLIST if key in os.environ}

def valid_cwd(path):
    return path if path and os.path.isdir(path) else None

def truncate_text_to_bytes(text, max_bytes):
    raw = str(text or "").encode("utf-8")
    if len(raw) <= max_bytes:
        return str(text or "")
    suffix = b"\n[prompt truncated to fit OS argv limit]"
    budget = max(max_bytes - len(suffix), 0)
    if budget == 0:
        return suffix[:max_bytes].decode("utf-8", errors="ignore")
    trimmed = raw[:budget].decode("utf-8", errors="ignore")
    return trimmed + suffix.decode("utf-8")

def build_agy_command(
    model_name,
    prompt,
    workspaces=None,
    target_cwd=None,
    conversation_id=None,
    print_timeout=None,
    disable_slash_commands=False,
    output_format="stream-json",
    skip_permissions=True,
    sandbox=False,
):
    cmd = [resolve_agy_bin()]
    if skip_permissions:
        cmd.append("--dangerously-skip-permissions")
    if sandbox:
        cmd.append("--sandbox")
    if print_timeout:
        cmd.extend(["--print-timeout", str(print_timeout)])
    cmd.extend(["--model", model_name, "--output-format", output_format])
    if disable_slash_commands:
        cmd.append("--disable-slash-commands")
    seen_dirs = set()
    for directory in [*(workspaces or []), target_cwd]:
        if not directory:
            continue
        norm = os.path.normpath(directory)
        if norm not in seen_dirs and os.path.isdir(directory):
            seen_dirs.add(norm)
            cmd.extend(["--add-dir", directory])
    if conversation_id:
        cmd.extend(["--conversation", conversation_id])
    cmd.extend(["--print", prompt])
    return cmd

def parse_agy_json_output(raw_stdout):
    text = (raw_stdout or "").strip()
    if not text:
        return None
    for candidate in [text, *reversed(text.splitlines())]:
        candidate = candidate.strip()
        if not candidate.startswith("{"):
            continue
        try:
            parsed = json.loads(candidate)
        except (json.JSONDecodeError, RecursionError, ValueError):
            continue
        if not isinstance(parsed, dict):
            continue
        ev_name = parsed.get("event")
        if isinstance(ev_name, str):
            if ev_name == "result" and isinstance(parsed.get("result"), dict):
                return parsed["result"]
            if ev_name in {"init", "step_update"}:
                continue
        elif ev_name is not None:
            continue
        return parsed
    return None

def classify_error_code(returncode, parsed, raw_stderr, timed_out=False, tool_disallowed=False, framing_lost=False):
    if timed_out:
        return "timeout"
    if tool_disallowed:
        return "tool_disallowed"
    if framing_lost:
        return "stream_framing_error"
    if not isinstance(parsed, dict):
        return "protocol_error" if returncode == 0 else "subprocess_exit"
    error_text = str(parsed.get("error") or "")
    stderr_text = str(raw_stderr or "")
    combined = f"{error_text}\n{stderr_text}".lower()
    finished_reason = str(parsed.get("finished_reason") or "").strip().lower()
    response_text = str(parsed.get("response") or "").lstrip().lower()
    if "invalid model selection" in combined or "not recognized as a known model" in combined:
        return "invalid_model"
    if response_text.startswith(SAFETY_BLOCK_PREFIX.lower()) or finished_reason in {"safety", "blocked"}:
        return "safety_blocked"
    if finished_reason == "print_timeout":
        return "timeout"
    if "conversation" in combined and any(w in combined for w in ("corrupt", "failed", "invalid", "unable", "error", "not found")):
        return "conversation_error"
    if parsed.get("error") or str(parsed.get("status") or "").strip().upper() != "SUCCESS":
        return "subprocess_exit" if returncode not in (0, None) else "protocol_error"
    return "subprocess_exit"

def evaluate_agy_result(returncode, parsed, raw_stderr, require_conversation_id=False, init_conversation_id=None):
    if not isinstance(parsed, dict):
        if returncode != 0:
            detail = (raw_stderr or "").strip() or f"Agent failed with code {returncode}"
            return False, redact_sensitive(detail)
        return False, "Missing or malformed JSON result envelope"

    status = str(parsed.get("status") or "").strip().upper()
    error = parsed.get("error")
    finished_reason = str(parsed.get("finished_reason") or "").strip().lower()
    response_text = str(parsed.get("response") or "").lstrip()

    if returncode != 0:
        detail = error or raw_stderr or f"Agent failed with code {returncode}"
        return False, redact_sensitive(detail).strip()
    if status != "SUCCESS":
        detail = error or f"Agent returned status {status or 'MISSING'}"
        return False, redact_sensitive(detail).strip()
    if error:
        return False, redact_sensitive(error).strip()
    if finished_reason in {"error", "safety", "blocked", "print_timeout"}:
        return False, f"Agent finished with reason {finished_reason}"
    if response_text.lower().startswith(SAFETY_BLOCK_PREFIX.lower()):
        return False, "Agent request was blocked by safety filters"

    usage = parsed.get("usage")
    if not isinstance(usage, dict):
        return False, "Malformed or missing usage payload in JSON result"
    total_tokens = usage.get("total_tokens")
    if not isinstance(total_tokens, int) or isinstance(total_tokens, bool) or total_tokens < 0:
        return False, "Malformed total_tokens in JSON result"
    for tok_field in ("input_tokens", "output_tokens"):
        if tok_field in usage and usage.get(tok_field) is not None:
            tok_val = usage.get(tok_field)
            if not isinstance(tok_val, int) or isinstance(tok_val, bool) or tok_val < 0:
                return False, f"Malformed {tok_field} in JSON result"

    raw_cid = parsed.get("conversation_id")
    if raw_cid not in (None, ""):
        if validate_conversation_id(raw_cid) is None:
            return False, "Malformed conversation_id in JSON result"

    if require_conversation_id:
        if raw_cid not in (None, ""):
            cid = validate_conversation_id(raw_cid)
        else:
            cid = validate_conversation_id(init_conversation_id)
        if not cid:
            return False, "Missing or malformed conversation_id in JSON result"
    return True, None

def should_drop_conversation_on_retry(error_reason, raw_stderr):
    combined = f"{error_reason or ''}\n{raw_stderr or ''}".lower()
    if "conversation" not in combined:
        return False
    return any(word in combined for word in ("corrupt", "failed", "invalid", "unable", "error", "not found"))

async def _read_bounded_stderr(stderr_stream, max_bytes=65536):
    if not stderr_stream:
        return b""
    chunks = []
    total = 0
    while True:
        chunk = await stderr_stream.read(4096)
        if not chunk:
            break
        if total < max_bytes:
            take = chunk[: max_bytes - total]
            chunks.append(take)
            total += len(take)
    return b"".join(chunks)

async def _iter_stream_lines(stream, max_line_bytes=None, partial_buf=None):
    if stream is None:
        return
    if max_line_bytes is None:
        max_line_bytes = MAX_STREAM_LINE_BYTES
    buf = partial_buf if partial_buf is not None else bytearray()
    discarding_oversized = False
    while True:
        chunk = await stream.read(65536)
        if not chunk:
            if not discarding_oversized and buf:
                line = bytes(buf)
                buf.clear()
                yield line, False
            return
        start = 0
        while start < len(chunk):
            nl = chunk.find(b"\n", start)
            if discarding_oversized:
                if nl == -1:
                    break
                discarding_oversized = False
                start = nl + 1
                continue
            if nl == -1:
                segment = chunk[start:]
                if len(buf) + len(segment) > max_line_bytes:
                    buf.clear()
                    discarding_oversized = True
                    yield None, True
                else:
                    buf.extend(segment)
                break
            segment = chunk[start:nl]
            if len(buf) + len(segment) > max_line_bytes:
                buf.clear()
                yield None, True
            else:
                if buf:
                    buf.extend(segment)
                    line = bytes(buf)
                    buf.clear()
                else:
                    line = segment
                yield line, False
            start = nl + 1

async def consume_agy_stream(process, timeout_seconds, on_conversation_id=None, allow_tools=True):
    init_conversation_id = None
    result_payload = None
    saw_tool_call = False
    tool_disallowed = False
    framing_lost = False
    timed_out = False
    stdout_lines = deque()
    stdout_bytes_retained = 0
    max_retained_stdout = 262144
    stderr_bytes = b""
    partial_buf = bytearray()

    def _retain_stdout_line(line):
        nonlocal stdout_bytes_retained
        line_len = len(line) + 1
        if line_len > max_retained_stdout:
            return
        stdout_lines.append(line)
        stdout_bytes_retained += line_len
        while stdout_bytes_retained > max_retained_stdout and stdout_lines:
            dropped = stdout_lines.popleft()
            stdout_bytes_retained -= len(dropped) + 1

    def _mark_framing_lost():
        nonlocal framing_lost, tool_disallowed
        framing_lost = True
        if not allow_tools:
            tool_disallowed = True
            return True
        return False

    def _process_line(line):
        nonlocal init_conversation_id, result_payload, saw_tool_call, tool_disallowed
        if not line.startswith("{"):
            return _mark_framing_lost()
        try:
            ev = json.loads(line)
        except (json.JSONDecodeError, RecursionError, ValueError):
            return _mark_framing_lost()
        if not isinstance(ev, dict):
            return _mark_framing_lost()
        ev_name = ev.get("event")
        if ev_name == "init":
            cid = validate_conversation_id(ev.get("conversation_id"))
            if cid:
                init_conversation_id = cid
                if on_conversation_id:
                    on_conversation_id(cid)
        elif ev_name == "step_update":
            step = ev.get("step_update")
            if not isinstance(step, dict):
                return _mark_framing_lost()
            raw_step_type = step.get("step_type")
            if not isinstance(raw_step_type, str) or not raw_step_type.strip():
                return _mark_framing_lost()
            step_type_norm = raw_step_type.strip().lower()
            if step_type_norm == "tool" or step_type_norm.startswith("tool_"):
                saw_tool_call = True
                if not allow_tools:
                    tool_disallowed = True
                    return True
        elif ev_name == "result":
            res_obj = ev.get("result")
            if not isinstance(res_obj, dict):
                return _mark_framing_lost()
            result_payload = res_obj
        elif ev_name is None and ("status" in ev or "error" in ev) and "step_update" not in ev:
            result_payload = ev
        else:
            if (
                not isinstance(ev_name, str)
                or not ev_name.strip()
                or "step_update" in ev
                or "tool" in ev_name.lower()
            ):
                return _mark_framing_lost()
        return False

    async def _drain_stdout():
        nonlocal framing_lost, tool_disallowed
        async for line_bytes, was_oversized in _iter_stream_lines(process.stdout, partial_buf=partial_buf):
            if was_oversized:
                framing_lost = True
                if not allow_tools:
                    tool_disallowed = True
                    await terminate_process(process)
                    return
                continue
            try:
                line = line_bytes.decode("utf-8", errors="strict").strip()
            except UnicodeDecodeError:
                framing_lost = True
                if not allow_tools:
                    tool_disallowed = True
                    await terminate_process(process)
                    return
                continue
            if not line:
                continue
            _retain_stdout_line(line)
            if _process_line(line):
                await terminate_process(process)
                return

    stderr_task = None
    if getattr(process, "stdout", None) is not None:
        stderr_task = asyncio.create_task(_read_bounded_stderr(getattr(process, "stderr", None)))

    async def _run_to_completion():
        nonlocal stderr_bytes, framing_lost, tool_disallowed
        if stderr_task is not None:
            await _drain_stdout()
            if process.returncode is None:
                await process.wait()
        else:
            out_b, err_b = await process.communicate()
            for raw_line in (out_b or b"").splitlines():
                if len(raw_line) > MAX_STREAM_LINE_BYTES:
                    framing_lost = True
                    if not allow_tools:
                        tool_disallowed = True
                        break
                    continue
                try:
                    line = raw_line.decode("utf-8", errors="strict").strip()
                except UnicodeDecodeError:
                    framing_lost = True
                    if not allow_tools:
                        tool_disallowed = True
                        break
                    continue
                if not line:
                    continue
                _retain_stdout_line(line)
                if _process_line(line):
                    break
            stderr_bytes = (err_b or b"")[:65536]

    try:
        try:
            await asyncio.wait_for(_run_to_completion(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            timed_out = True
            if bytes(partial_buf).strip():
                framing_lost = True
                if not allow_tools:
                    tool_disallowed = True
            await terminate_process(process)

        if stderr_task is not None:
            if not stderr_task.done():
                try:
                    stderr_bytes = await asyncio.wait_for(stderr_task, timeout=0.5)
                except Exception:
                    stderr_task.cancel()
                    await terminate_process(process)
            elif not stderr_task.cancelled():
                stderr_bytes = stderr_task.result()
    finally:
        if stderr_task is not None and not stderr_task.done():
            stderr_task.cancel()
            try:
                await stderr_task
            except BaseException:
                pass

    raw_stdout = "\n".join(stdout_lines)
    raw_stderr = stderr_bytes.decode("utf-8", errors="replace")
    if result_payload is None and not framing_lost and not tool_disallowed:
        result_payload = parse_agy_json_output(raw_stdout)
    return {
        "returncode": process.returncode,
        "init_conversation_id": init_conversation_id,
        "result": result_payload,
        "saw_tool_call": saw_tool_call,
        "tool_disallowed": tool_disallowed,
        "framing_lost": framing_lost,
        "timed_out": timed_out,
        "raw_stdout": raw_stdout,
        "raw_stderr": raw_stderr,
    }

def is_retryable_pre_tool_failure(stream_res, parsed, error_code=None):
    if stream_res.get("timed_out") or stream_res.get("saw_tool_call") or stream_res.get("framing_lost"):
        return False
    if error_code in {"timeout", "tool_disallowed", "stream_framing_error", "safety_blocked", "invalid_model"}:
        return False
    if isinstance(parsed, dict):
        finished_reason = str(parsed.get("finished_reason") or "").strip().lower()
        if finished_reason in {"print_timeout", "timeout", "safety", "blocked"}:
            return False
        if parsed.get("error") or str(parsed.get("status") or "").strip().upper() == "ERROR":
            return True
    if stream_res.get("returncode") not in (0, None) and not stream_res.get("init_conversation_id"):
        return True
    return False

def parse_classifier_phase(response_text):
    cleaned = re.sub(r"^[`*\s]+|[`*\s.!?:]+$", "", str(response_text or "").strip()).upper()
    return cleaned if cleaned in VALID_PHASES else "GENERAL"

def build_classifier_prompt(comment, card_name, card_desc, list_name, action_type=None, max_bytes=None):
    limit = MAX_PROMPT_BYTES if max_bytes is None else max_bytes
    safe_action = truncate_text_to_bytes(action_type or "unknown", 64)
    safe_list = truncate_text_to_bytes(list_name or "", 128)
    safe_name = truncate_text_to_bytes(card_name or "", 256)
    header = (
        "Do NOT call any tools, read files, or run commands.\n"
        "Classify the following Trello trigger semantically to determine the expected phase.\n\n"
    )
    footer = (
        "\n\nRespond with exactly one word (no punctuation, no explanation, in uppercase):\n"
        "- READY_FOR_SPEC (if the list name is 'Ready for Spec' or 'Ready for Specification', or if the comment or description indicates requirements are finalized, locked in, ready to build/spec, or requests writing/creating a spec or GitHub issues)\n"
        "- INVESTIGATOR (if the list name is 'Ideas', 'Research', 'In Progress', or if the comment or card status asks for reviews, architectural suggestions, feedback, options, or is an early stage description that needs refinement/information)\n"
        "- GENERAL (if the trigger is conversational, generic testing, status updates, asks a simple follow-up question, asks for clarification on existing work, or is discussion not requesting a new spec/GitHub issue)"
    )
    fixed_meta = (
        f"Trigger Action Type: {safe_action}\n"
        f"Card Name: {safe_name}\n"
        f"List Name: {safe_list}\n"
    )
    raw_comment = str(comment or "").strip() or "(none - non-comment event)"
    static_overhead = len((header + fixed_meta + "Card Description: \nTriggering Comment: " + footer).encode("utf-8"))
    if static_overhead > limit:
        raise ValueError(
            f"Classifier prompt limit ({limit} bytes) is smaller than required header/footer ({static_overhead} bytes)"
        )
    remaining = limit - static_overhead
    comment_cap = min(8192, max(256, remaining // 2)) if remaining >= 256 else remaining
    safe_comment = truncate_text_to_bytes(raw_comment, comment_cap)
    desc_budget = max(remaining - len(safe_comment.encode("utf-8")), 0)
    safe_desc = truncate_text_to_bytes(card_desc or "", desc_budget) if desc_budget > 0 else ""
    return (
        f"{header}"
        f"{fixed_meta}"
        f"Card Description: {safe_desc}\n"
        f"Triggering Comment: {safe_comment}"
        f"{footer}"
    )

async def classify_action(comment, card_name, card_desc, list_name, action_type=None):
    """Performs a quick classification run to determine the card's phase semantically."""
    clean_list = str(list_name).strip().lower()
    if clean_list in ["ready for spec", "ready for specification"]:
        logging.info(f"[Trello Sidecar] Automatically classified phase as READY_FOR_SPEC due to list name: '{list_name}'")
        return "READY_FOR_SPEC"

    clean_comment = normalize_trigger_text(comment)
    simple_followup_patterns = [
        r"\?$",
        r"\b(why|what|where|when|who|how|isn'?t|aren'?t|don'?t|does|do|can|could|should)\b",
        r"\b(looks good|lgtm|thanks|thank you|approved|ship it|go ahead|never responded|any update)\b",
    ]
    spec_request_pattern = r"\b(spec|github issue|create issue|ready for spec|ready to spec|write.*spec|locked in|requirements are final)\b"
    if clean_comment and not re.search(spec_request_pattern, clean_comment):
        if len(clean_comment) <= 240 and any(re.search(pattern, clean_comment) for pattern in simple_followup_patterns):
            logging.info("[Trello Sidecar] Classified as GENERAL due to short follow-up/question comment.")
            return "GENERAL"

    classify_prompt = build_classifier_prompt(
        comment,
        card_name,
        card_desc,
        list_name,
        action_type=action_type,
        max_bytes=MAX_PROMPT_BYTES,
    )
    cmd = build_agy_command(
        CLASSIFIER_MODEL,
        classify_prompt,
        workspaces=None,
        target_cwd=None,
        conversation_id=None,
        print_timeout=CLASSIFIER_PRINT_TIMEOUT,
        disable_slash_commands=True,
        output_format="stream-json",
        skip_permissions=False,
        sandbox=True,
    )
    logging.info("[Trello Sidecar] Classifying trigger comment semantically...")
    process = None
    launch_started = False
    try:
        with tempfile.TemporaryDirectory(prefix="agy-cls-") as cls_cwd:
            with _spawn_gate_lock:
                if not begin_process_launch():
                    raise ShutdownRequested("Server shutting down")
                launch_started = True
                try:
                    process = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        cwd=cls_cwd,
                        env=build_classifier_env(),
                        start_new_session=True,
                        limit=1024 * 1024,
                    )
                finally:
                    spawn_still_open = finish_process_launch(process)
                    launch_started = False
            if not spawn_still_open or shutdown_event.is_set():
                await terminate_process(process)
                raise ShutdownRequested("Server shutting down")
            stream_res = await consume_agy_stream(
                process,
                CLASSIFIER_TIMEOUT_SECONDS,
                allow_tools=False,
            )
        if stream_res["tool_disallowed"] or stream_res["framing_lost"]:
            logging.error("[Trello Sidecar] Classifier attempted a disallowed tool call or lost stream framing; blocking job.")
            return PHASE_BLOCKED_TOOL_ATTEMPT
        if stream_res["timed_out"]:
            logging.error(
                "[Trello Sidecar] Classification timed out after %ss; falling back to GENERAL.",
                CLASSIFIER_TIMEOUT_SECONDS,
            )
            return "GENERAL"

        parsed = stream_res["result"]
        ok, _ = evaluate_agy_result(stream_res["returncode"], parsed, stream_res["raw_stderr"])
        if ok:
            response_text = str((parsed or {}).get("response") or "")
            phase = parse_classifier_phase(response_text)
            logging.info(
                "[Trello Sidecar] Semantic classification resolved phase=%s (response_len=%d)",
                phase,
                len(response_text),
            )
            return phase
        error_code = classify_error_code(
            stream_res["returncode"],
            parsed,
            stream_res["raw_stderr"],
            timed_out=stream_res["timed_out"],
            tool_disallowed=stream_res["tool_disallowed"],
            framing_lost=stream_res["framing_lost"],
        )
        logging.error(
            "[Trello Sidecar] Classification process failed (rc=%s, error_code=%s)",
            stream_res["returncode"],
            error_code,
        )
    except ShutdownRequested:
        raise
    except (RecursionError, UnicodeDecodeError, json.JSONDecodeError) as e:
        logging.error("[Trello Sidecar] Classifier stream decoding failure (%s); blocking job.", type(e).__name__)
        return PHASE_BLOCKED_TOOL_ATTEMPT
    except Exception as e:
        logging.error("[Trello Sidecar] Exception during classification: %s", type(e).__name__)
    finally:
        if launch_started:
            finish_process_launch(process)
        if process:
            await terminate_process(process)
            unregister_active_process(process)
    return "GENERAL"

def extract_short_id(card_link):
    if not card_link:
        return None
    match = re.search(r'/c/([a-zA-Z0-9]+)', card_link)
    if match:
        return match.group(1)
    return None

def extract_mentioned_usernames(text):
    if not text:
        return []
    # Trello automation may markdown-bold mentions as @**username**.
    mentions = re.findall(r'@\*\*([A-Za-z0-9_]+)\*\*|@([A-Za-z0-9_]+)', text)
    usernames = []
    for bold_match, plain_match in mentions:
        username = (bold_match or plain_match or "").strip().lower()
        if username and username not in usernames:
            usernames.append(username)
    return usernames

def resolve_git_repo(directory):
    try:
        res = subprocess.run(
            ["git", "-C", directory, "remote", "get-url", "origin"],
            capture_output=True, text=True, timeout=3
        )
        if res.returncode == 0:
            url = res.stdout.strip()
            match = re.search(r'github\.com[:/]([^/]+/[^.]+)', url)
            if match:
                return match.group(1)
    except Exception:
        pass
    return None

def extract_action(payload):
    action_data = payload.get("action")
    return action_data if isinstance(action_data, dict) else {}

def extract_trigger_member(payload):
    if not isinstance(payload, dict):
        return "", "Unknown"
    action_data = extract_action(payload)
    member_creator = action_data.get("memberCreator")
    if isinstance(member_creator, dict):
        raw_user = _safe_str(member_creator.get("username"))
        if raw_user:
            username = raw_user.lower()
            name = _safe_str(member_creator.get("fullName")) or username
            return username, name
    raw_user = _safe_str(payload.get("triggerUserUsername"))
    username = raw_user.lower() if raw_user else ""
    creator_full = _safe_str(member_creator.get("fullName")) if isinstance(member_creator, dict) else None
    name = _safe_str(payload.get("triggerUserName")) or creator_full or username or "Unknown"
    return username, name

def extract_recent_comments_from_card_data(card_data, exclude_action_id=None, limit=12):
    comments = []
    for action in card_data.get("actions", []) or []:
        if action.get("type") != "commentCard":
            continue
        if exclude_action_id and action.get("id") == exclude_action_id:
            continue
        text = action.get("data", {}).get("text")
        if not text:
            continue
        comments.append({
            "date": action.get("date", ""),
            "text": text,
            "memberCreator": action.get("memberCreator", {}),
        })
    comments.sort(key=lambda item: item.get("date", ""))
    return comments[-limit:]

def extract_relay_delivery_key(payload):
    if not isinstance(payload, dict):
        return None
    for key in ("deliveryId", "eventId", "webhookEventId", "relayDeliveryId", "idempotencyKey"):
        val = _safe_str(payload.get(key))
        if val:
            return val
    return None

def extract_action_id(payload):
    if not isinstance(payload, dict):
        return None
    action_data = extract_action(payload)
    return (
        _safe_str(action_data.get("id"))
        or _safe_str(payload.get("actionId"))
        or _safe_str(payload.get("idAction"))
    )

def extract_action_type(payload):
    if not isinstance(payload, dict):
        return "unknown"
    action_data = extract_action(payload)
    return _safe_str(action_data.get("type")) or _safe_str(payload.get("actionType")) or "unknown"

def trigger_event_details(card_key, card_name, payload, comment, list_name, reason=None):
    username, display_name = extract_trigger_member(payload)
    details = {
        "card_id": str(card_key or ""),
        "card_name": str(card_name or ""),
        "card_link": payload.get("cardLink"),
        "action_id": extract_action_id(payload),
        "action_type": extract_action_type(payload),
        "member_username": username,
        "member_name": display_name,
        "list_name": list_name,
        "comment_hash": hashlib.sha256(str(comment or "").encode("utf-8")).hexdigest() if comment else None,
        "comment_length": len(str(comment or "")),
        "probable_agent_comment": is_probable_agent_comment(comment, username, display_name),
    }
    if reason:
        details["reason"] = reason
    return details

def log_trigger_ignored(card_key, card_name, payload, comment, list_name, reason):
    log_sidecar_event("trigger_ignored", trigger_event_details(card_key, card_name, payload, comment, list_name, reason))

def log_trigger_accepted(card_key, card_name, payload, comment, list_name):
    log_sidecar_event("trigger_accepted", trigger_event_details(card_key, card_name, payload, comment, list_name))

def normalize_trigger_text(text):
    return re.sub(r"\s+", " ", str(text or "")).strip().lower()

NOVELTY_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "been", "but", "by",
    "can", "could", "do", "does", "for", "from", "got", "have", "i",
    "if", "in", "is", "it", "its", "let", "me", "of", "on", "or",
    "our", "should", "so", "that", "the", "their", "this", "to",
    "we", "with", "you", "your",
}

LOW_SIGNAL_AGENT_PATTERNS = [
    r"\b(agreed|agree|ok|okay|thanks|thank you|sounds good|looks good|lgtm|got it|checking|will check|let me know)\b",
    r"^\s*(?:@\*\*?[A-Za-z0-9_]+\*\*?|@[A-Za-z0-9_]+|\s|[,!.])+?\s*$",
]

SUBSTANCE_SIGNAL_PATTERNS = [
    r"\?",
    r"https?://",
    r"\b(issue|pr|pull request|github|trello|screenshot|mockup|figma|route|url|repro|steps?|error|bug|broken|decision|approve|approved|blocked|acceptance|criterion|criteria|requirement|scope|question|answer|confirm|choose|option|ship|deploy|staging)\b",
    r"`[^`]+`",
]

def strip_agent_markers(text):
    stripped = str(text or "")
    legacy_markers = ["[agy-sidecar:comment]", "[agy-sidecar:ack]", "[agy-sidecar:status]"]
    current_markers = [AGENT_COMMENT_MARKER, AGENT_ACK_MARKER, AGENT_STATUS_MARKER]
    all_markers = list(set(current_markers + legacy_markers))
    for marker in all_markers:
        if marker:
            stripped = stripped.replace(marker, " ")
    stripped = re.sub(SUPPRESSED_COMMENT_REGEX, " ", stripped)
    return stripped

def normalized_novelty_text(text):
    text = strip_agent_markers(text)
    text = re.sub(r"@\*\*([A-Za-z0-9_]+)\*\*|@([A-Za-z0-9_]+)", " ", text)
    text = re.sub(r"(?im)^\s*-\s*love\s+\w+\s*$", " ", text)
    text = re.sub(r"https?://\S+", " URL ", text)
    text = re.sub(r"[^a-zA-Z0-9_/?#.-]+", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()

def novelty_tokens(text):
    tokens = set()
    for token in re.findall(r"[a-zA-Z0-9_#/.?-]+", normalized_novelty_text(text)):
        token = token.strip(".,!?;:()[]{}")
        if len(token) < 3 or token in NOVELTY_STOPWORDS:
            continue
        tokens.add(token)
    return tokens

def has_substance_signal(text):
    normalized = normalized_novelty_text(text)
    return any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in SUBSTANCE_SIGNAL_PATTERNS)

def is_probable_agent_comment(comment, username=None, display_name=None):
    text = str(comment or "")
    legacy_markers = ["[agy-sidecar:comment]", "[agy-sidecar:ack]", "[agy-sidecar:status]"]
    current_markers = [AGENT_COMMENT_MARKER, AGENT_ACK_MARKER, AGENT_STATUS_MARKER]
    all_markers = list(set(current_markers + legacy_markers))
    if any(marker and marker in text for marker in all_markers):
        return True
    for identity in [username, display_name]:
        if not identity:
            continue
        try:
            if re.search(AGENT_COMMENT_USER_REGEX, str(identity)):
                return True
        except re.error as e:
            logging.warning("[Trello Sidecar] Invalid TRELLO_AGENT_COMMENT_USER_REGEX: %s", e)
            return False
    return False

def extract_recent_comment_texts(payload):
    recent = []
    for entry in payload.get("recentComments", []) or []:
        if isinstance(entry, dict):
            text = entry.get("text") or entry.get("comment") or entry.get("body")
        else:
            text = entry
        if text:
            recent.append(str(text))
    return recent[-12:]

def is_low_signal_agent_comment(comment, recent_comments):
    tokens = novelty_tokens(comment)
    if not tokens:
        return True

    normalized = normalized_novelty_text(comment)
    if not has_substance_signal(comment):
        if any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in LOW_SIGNAL_AGENT_PATTERNS):
            return True
        if len(tokens) < 4:
            return True

    recent_token_sets = [novelty_tokens(text) for text in recent_comments if text]
    recent_token_sets = [token_set for token_set in recent_token_sets if token_set]
    if not recent_token_sets:
        return False

    recent_union = set().union(*recent_token_sets)
    unique_ratio = len(tokens - recent_union) / max(len(tokens), 1)
    max_overlap = max(
        len(tokens & recent_tokens) / max(len(tokens | recent_tokens), 1)
        for recent_tokens in recent_token_sets
    )
    return unique_ratio < LOW_NOVELTY_UNIQUE_TOKEN_RATIO or max_overlap >= LOW_NOVELTY_OVERLAP_RATIO

def build_ack_comment():
    return f"Got it, I am checking this now.\n\n{AGENT_ACK_MARKER}"

def build_failure_comment():
    return (
        "I hit a technical snag processing this request and couldn't complete it — "
        "please re-trigger or ping a human.\n\n"
        f"{AGENT_STATUS_MARKER}"
    )

def should_post_ack_comment(card_key, phase, state, now=None):
    if phase not in {"INVESTIGATOR", "READY_FOR_SPEC"}:
        return False
    now = time.time() if now is None else now
    last_ack_by_card = state.setdefault("last_ack_by_card", {})
    last_ack = float(last_ack_by_card.get(str(card_key), 0) or 0)
    return now - last_ack >= ACK_COOLDOWN_SECONDS

def record_ack_comment(card_key):
    key_str = str(normalize_canonical_trello_id(card_key) or card_key)
    with state_lock:
        try:
            state = load_sidecar_state_strict()
        except SidecarStateReadError:
            return False
        state.setdefault("last_ack_by_card", {})[key_str] = time.time()
        return save_sidecar_state(state)

def _matches_admission(entry, admission_id=None, action_id=None):
    if not isinstance(entry, dict) or entry.get("stage") != "queued":
        return False
    entry_adm = entry.get("admission_id")
    if entry_adm is not None or admission_id is not None:
        if entry_adm != admission_id:
            return False
    entry_action = entry.get("action_id")
    if entry_action is not None and action_id is not None:
        if entry_action != action_id:
            return False
    return True

def reserve_ack_comment(card_key, phase, action_id=None, admission_id=None, action_id_inferred=False, short_card_id=None):
    if not POST_ACK_COMMENTS:
        return False
    key_str = str(normalize_canonical_trello_id(card_key) or card_key)
    with state_lock:
        try:
            state = load_sidecar_state_strict()
        except SidecarStateReadError:
            log_sidecar_event("ack_reservation_failed", {"card_id": key_str, "phase": phase})
            return False
        in_flight = state.get("in_flight_cards")
        in_flight_entry = in_flight.get(key_str) if isinstance(in_flight, dict) else None
        if not _matches_admission(in_flight_entry, admission_id=admission_id, action_id=action_id):
            if action_id and is_quarantined_in_state(state, action_id=action_id, action_id_inferred=action_id_inferred):
                logging.warning(
                    "[Trello Sidecar] Refusing ack reservation for card '%s': action %s is quarantined.",
                    key_str,
                    action_id,
                )
                log_sidecar_event(
                    "ack_reservation_failed",
                    {"card_id": key_str, "phase": phase, "reason": "unreconciled_action"},
                )
                return False
            logging.warning(
                "[Trello Sidecar] Refusing ack reservation for card '%s': missing or mismatched queued admission.",
                key_str,
            )
            log_sidecar_event(
                "ack_reservation_failed",
                {"card_id": key_str, "phase": phase, "reason": "missing_or_mismatched_admission"},
            )
            return False
        clean_short = _safe_str(short_card_id)
        q_act, q_inf, q_dkeys, q_rfps, q_lfps, _ = _extract_row_quarantine_identities(
            state,
            key_str,
            in_flight_entry,
            action_id=action_id,
            action_id_inferred=action_id_inferred if action_id else None,
            extra_shorts=[clean_short] if clean_short else (),
        )
        if is_quarantined_in_state(
            state,
            action_id=q_act,
            action_id_inferred=q_inf,
            delivery_keys=q_dkeys,
            replay_fingerprints=q_rfps,
            legacy_fingerprints=q_lfps,
        ):
            logging.warning(
                "[Trello Sidecar] Refusing ack reservation for card '%s': matched quarantined trigger (action=%s).",
                key_str,
                q_act or "id-less",
            )
            log_sidecar_event(
                "ack_reservation_failed",
                {"card_id": key_str, "phase": phase, "reason": "unreconciled_action"},
            )
            return False
        if in_flight_entry.get("ack_reserved"):
            return False
        acked_actions = state.get("acked_action_ids")
        if not isinstance(acked_actions, list):
            acked_actions = []
            state["acked_action_ids"] = acked_actions
        if action_id and action_id in acked_actions:
            return False
        if not should_post_ack_comment(key_str, phase, state):
            return False
        state.setdefault("last_ack_by_card", {})[key_str] = time.time()
        in_flight_entry["ack_reserved"] = True
        if action_id:
            acked_actions.append(action_id)
            state["acked_action_ids"] = acked_actions[-500:]
        if not save_sidecar_state(state):
            logging.error("[Trello Sidecar] Failed to persist ack cooldown state for card '%s'; skipping ack post.", key_str)
            log_sidecar_event("ack_reservation_failed", {"card_id": key_str, "phase": phase})
            return False
        return True

def record_verified_card_alias(short_id, canonical_id, card_url=None):
    short_key = str(short_id or "").strip()
    norm_id = normalize_canonical_trello_id(canonical_id)
    if not short_key or not norm_id:
        return False
    target_url = str(card_url or "").strip() or f"https://trello.com/c/{short_key}"
    with state_lock:
        try:
            state = load_sidecar_state_strict()
        except SidecarStateReadError:
            return False
        aliases = state.setdefault("card_aliases", {})
        if not isinstance(aliases, dict):
            aliases = {}
            state["card_aliases"] = aliases
        cards_cache = state.setdefault("canonical_cards", {})
        if not isinstance(cards_cache, dict):
            cards_cache = {}
            state["canonical_cards"] = cards_cache
        existing_card = cards_cache.get(norm_id)
        desired_card = {"short_link": short_key, "url": target_url}
        if aliases.get(short_key) == norm_id and existing_card == desired_card:
            return True
        aliases[short_key] = norm_id
        if len(aliases) > 2000:
            for oldest_key in list(aliases.keys())[:-2000]:
                aliases.pop(oldest_key, None)
        cards_cache[norm_id] = desired_card
        if len(cards_cache) > 2000:
            for oldest_key in list(cards_cache.keys())[:-2000]:
                cards_cache.pop(oldest_key, None)
        return save_sidecar_state(state)

def lookup_verified_canonical_id(short_id):
    short_key = str(short_id or "").strip()
    if not short_key:
        return None
    with state_lock:
        try:
            state = load_sidecar_state_strict()
        except SidecarStateReadError:
            return None
        aliases = state.get("card_aliases") or {}
        if isinstance(aliases, dict):
            return normalize_canonical_trello_id(aliases.get(short_key))
    return None

def lookup_verified_card_info(canonical_id):
    norm_id = normalize_canonical_trello_id(canonical_id)
    if not norm_id:
        return None
    with state_lock:
        try:
            state = load_sidecar_state_strict()
        except SidecarStateReadError:
            return None
        cards_cache = state.get("canonical_cards") or {}
        if isinstance(cards_cache, dict):
            entry = cards_cache.get(norm_id)
            if isinstance(entry, dict) and entry.get("short_link"):
                short_link = str(entry["short_link"]).strip()
                url = str(entry.get("url") or "").strip() or f"https://trello.com/c/{short_link}"
                return {"short_link": short_link, "url": url}
        aliases = state.get("card_aliases") or {}
        if isinstance(aliases, dict):
            for short_key, target_id in aliases.items():
                if normalize_canonical_trello_id(target_id) == norm_id and str(short_key).strip():
                    sk = str(short_key).strip()
                    return {"short_link": sk, "url": f"https://trello.com/c/{sk}"}
    return None

def _dedup_strs(items):
    out = []
    seen = set()
    for item in items or ():
        s = _safe_str(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out

def _extract_action_field_key(payload, field_name):
    if not isinstance(payload, dict):
        return ""
    action_data = extract_action(payload).get("data")
    if isinstance(action_data, dict):
        raw_val = action_data.get(field_name)
        if isinstance(raw_val, dict):
            val = _safe_str(raw_val.get("name")) or _safe_str(raw_val.get("id")) or _safe_str(raw_val.get("color"))
            if val:
                return normalize_trigger_text(val)
        else:
            val = _safe_str(raw_val)
            if val:
                return normalize_trigger_text(val)
    top_val = _safe_str(payload.get(field_name))
    if not top_val and field_name == "label":
        top_val = _safe_str(payload.get("labelName"))
    return normalize_trigger_text(top_val or "")

def trigger_fingerprint(card_key, payload, comment, list_name):
    payload_dict = payload if isinstance(payload, dict) else {}
    parts = [
        str(normalize_canonical_trello_id(card_key) or _safe_str(card_key) or ""),
        extract_action_type(payload_dict),
        normalize_trigger_text(comment),
        normalize_trigger_text(list_name),
        normalize_trigger_text(_safe_str(payload_dict.get("cardName")) or ""),
    ]
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()

def relay_replay_fingerprint(card_key, payload, comment, list_name, username=None):
    payload_dict = payload if isinstance(payload, dict) else {}
    action_obj = extract_action(payload_dict)
    action_data = action_obj.get("data") if isinstance(action_obj.get("data"), dict) else {}
    raw_card = action_data.get("card") if isinstance(action_data.get("card"), dict) else {}
    action_type = extract_action_type(payload_dict)
    card_part = str(normalize_canonical_trello_id(card_key) or _safe_str(card_key) or "")
    comment_norm = "" if action_type in NON_COMMENT_ACTION_TYPES else normalize_trigger_text(comment)
    list_norm = normalize_trigger_text(list_name)
    card_name_norm = normalize_trigger_text(
        _safe_str(payload_dict.get("cardName")) or _safe_str(raw_card.get("name")) or ""
    )
    raw_author = username if username is not None else extract_trigger_member(payload_dict)[0]
    author_norm = normalize_trigger_text(_safe_str(raw_author) or "")
    parts = [
        card_part,
        normalize_trigger_text(action_type),
        comment_norm,
        list_norm,
        card_name_norm,
        author_norm,
        _extract_action_field_key(payload_dict, "label"),
        _extract_action_field_key(payload_dict, "listBefore"),
        _extract_action_field_key(payload_dict, "listAfter"),
    ]
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()

def _verified_short_ids_for_card(state, norm_key, extra_verified_shorts=()):
    candidates = list(extra_verified_shorts or ())
    cards_cache = state.get("canonical_cards")
    if isinstance(cards_cache, dict):
        entry = cards_cache.get(norm_key)
        if isinstance(entry, dict):
            candidates.append(entry.get("short_link"))
    aliases = state.get("card_aliases")
    if isinstance(aliases, dict):
        for short_key, target_id in aliases.items():
            norm_target = normalize_canonical_trello_id(target_id)
            if norm_target == norm_key or str(target_id or "").strip() == norm_key:
                candidates.append(short_key)
    return _dedup_strs(candidates)

def _candidate_card_keys_for_identity(norm_key, pre_enrichment=None, verified_short_ids=()):
    card_keys = _dedup_strs([norm_key, *list(verified_short_ids or ())])
    if isinstance(pre_enrichment, dict):
        raw_cid = _safe_str(pre_enrichment.get("raw_card_id"))
        if raw_cid and not is_canonical_trello_id(raw_cid):
            if not verified_short_ids or raw_cid in verified_short_ids:
                card_keys = _dedup_strs([*card_keys, raw_cid])
    return card_keys

def _compute_candidate_delivery_keys(
    norm_key,
    payload,
    pre_enrichment=None,
    verified_short_ids=(),
):
    card_keys = _candidate_card_keys_for_identity(
        norm_key,
        pre_enrichment=pre_enrichment,
        verified_short_ids=verified_short_ids,
    )
    raw_dks = []
    top_dk = extract_relay_delivery_key(payload)
    if top_dk:
        raw_dks.append(top_dk)
    if isinstance(pre_enrichment, dict):
        pre_payload = pre_enrichment.get("payload")
        pre_dk = extract_relay_delivery_key(pre_payload) if isinstance(pre_payload, dict) else None
        if pre_dk:
            raw_dks.append(pre_dk)
    raw_dks = _dedup_strs(raw_dks)
    return _dedup_strs(f"{ckey}:{dk}" for ckey in card_keys for dk in raw_dks)

def _compute_candidate_fingerprints(
    norm_key,
    payload,
    comment,
    list_name,
    pre_enrichment=None,
    verified_short_ids=(),
):
    card_keys = _candidate_card_keys_for_identity(
        norm_key,
        pre_enrichment=pre_enrichment,
        verified_short_ids=verified_short_ids,
    )

    variants = [(payload, comment, list_name, None)]
    if isinstance(pre_enrichment, dict):
        pre_payload = pre_enrichment.get("payload")
        if isinstance(pre_payload, dict):
            variants.append((
                pre_payload,
                pre_enrichment.get("comment", comment),
                pre_enrichment.get("list_name", list_name),
                pre_enrichment.get("raw_username"),
            ))

    replay_fps = []
    legacy_fps = []
    for ckey in card_keys:
        for var_payload, var_comment, var_list, var_user in variants:
            replay_fps.append(
                relay_replay_fingerprint(ckey, var_payload, var_comment, var_list, username=var_user)
            )
            legacy_fps.append(
                trigger_fingerprint(ckey, var_payload, var_comment or "", var_list or "")
            )
    return _dedup_strs(replay_fps), _dedup_strs(legacy_fps)

def _is_legacy_headerless_entry(entry, entry_delivery_keys=()):
    if not isinstance(entry, dict):
        return False
    if entry.get("legacy_headerless"):
        return True
    if entry_delivery_keys:
        return False
    stage = _safe_str(entry.get("stage"))
    if not stage or stage == "unknown":
        return True
    if "delivery_keys" not in entry and not _safe_str(entry.get("admission_id")):
        return True
    return False

def _row_fingerprint_variants(payload):
    if not isinstance(payload, dict):
        return [({}, "", "")]
    raw_comment = payload.get("comment") if isinstance(payload.get("comment"), str) else ""
    raw_list = _safe_str(payload.get("listName")) or ""
    variants = [(payload, raw_comment, raw_list)]
    norm, _, _, _, _ = normalize_incoming_webhook(copy.deepcopy(payload), is_native_trello=False)
    if isinstance(norm, dict) and isinstance(norm.get("payload"), dict):
        norm_payload = norm["payload"]
        norm_comment = norm.get("comment") if isinstance(norm.get("comment"), str) else ""
        norm_list = _safe_str(norm.get("list_name")) or ""
        variants.append((norm_payload, norm_comment, norm_list))
    return variants

def _extract_row_quarantine_identities(
    state,
    norm_key,
    entry,
    action_id=None,
    action_id_inferred=None,
    raw_key=None,
    extra_shorts=(),
):
    entry = entry if isinstance(entry, dict) else {}
    payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
    raw_s = _safe_str(raw_key)
    persisted_short = raw_s if (raw_s and not is_canonical_trello_id(raw_s)) else None
    extra_list = [
        s for s in (
            _safe_str(entry.get("short_card_id")),
            persisted_short,
            *list(extra_shorts or ()),
        )
        if s
    ]
    if not is_canonical_trello_id(norm_key) and not extra_list:
        fallback_short = _safe_str(payload.get("cardShortId")) or extract_short_id(
            _safe_str(payload.get("cardLink")) or _safe_str(entry.get("card_link"))
        )
        if fallback_short:
            extra_list.append(fallback_short)
    verified_shorts = _verified_short_ids_for_card(state, norm_key, extra_verified_shorts=extra_list)
    card_keys = _dedup_strs([norm_key, *verified_shorts])

    row_action_id = _safe_str(action_id) or _safe_str(entry.get("action_id")) or extract_action_id(payload)
    if action_id_inferred is None:
        row_action_inferred = bool(entry.get("action_id_inferred")) if row_action_id else False
    else:
        row_action_inferred = bool(action_id_inferred) if row_action_id else False

    raw_dks = []
    top_dk = extract_relay_delivery_key(payload)
    if top_dk:
        raw_dks.append(top_dk)
    delivery_keys = _dedup_strs([
        *(entry.get("delivery_keys") or ()),
        *(f"{ckey}:{dk}" for ckey in card_keys for dk in raw_dks),
    ])

    replay_fps = [fp for fp in (entry.get("replay_fingerprints") or []) if isinstance(fp, str) and fp]
    legacy_fps = [fp for fp in (entry.get("legacy_fingerprints") or []) if isinstance(fp, str) and fp]
    if entry.get("fingerprint"):
        legacy_fps.append(entry["fingerprint"])
    variants = _row_fingerprint_variants(payload)
    for ckey in card_keys:
        for var_payload, var_comment, var_list in variants:
            replay_fps.append(relay_replay_fingerprint(ckey, var_payload, var_comment, var_list))
            legacy_fps.append(trigger_fingerprint(ckey, var_payload, var_comment, var_list))
    return (
        row_action_id,
        row_action_inferred,
        delivery_keys,
        _dedup_strs(replay_fps),
        _dedup_strs(legacy_fps),
        (verified_shorts[0] if verified_shorts else None),
    )

def _build_tombstone_from_detail(u_key, entry, state, now, legacy_fps_map=None):
    raw_card_id = _safe_str(entry.get("card_id")) or ""
    card_id = str(normalize_canonical_trello_id(raw_card_id) or raw_card_id)
    payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
    initial_short = _safe_str(entry.get("short_card_id"))
    if raw_card_id and not is_canonical_trello_id(raw_card_id) and not initial_short:
        initial_short = raw_card_id
    aliases = state.get("card_aliases") if isinstance(state.get("card_aliases"), dict) else {}
    if card_id and not is_canonical_trello_id(card_id):
        bridged = normalize_canonical_trello_id(aliases.get(card_id))
        if bridged:
            if not initial_short:
                initial_short = card_id
            card_id = bridged
    extra_shorts = [initial_short] if initial_short else []
    if card_id and not is_canonical_trello_id(card_id) and not extra_shorts:
        fallback_short = _safe_str(payload.get("cardShortId")) or extract_short_id(
            _safe_str(entry.get("card_link")) or _safe_str(payload.get("cardLink"))
        )
        if fallback_short:
            extra_shorts.append(fallback_short)
    verified_shorts = _verified_short_ids_for_card(state, card_id, extra_verified_shorts=extra_shorts)
    short_card_id = verified_shorts[0] if verified_shorts else None
    card_keys = _dedup_strs([card_id, *verified_shorts])

    action_id = _safe_str(entry.get("action_id")) or extract_action_id(payload)
    action_id_inferred = bool(entry.get("action_id_inferred")) if action_id else False
    delivery_keys = [dk for dk in (entry.get("delivery_keys") if isinstance(entry.get("delivery_keys"), list) else []) if isinstance(dk, str) and dk]
    raw_dk = extract_relay_delivery_key(payload)
    if raw_dk:
        for key_cand in card_keys:
            delivery_keys.append(f"{key_cand}:{raw_dk}")
    delivery_keys = _dedup_strs(delivery_keys)
    legacy_headerless = _is_legacy_headerless_entry(entry, entry_delivery_keys=delivery_keys)
    raw_fps = entry.get("replay_fingerprints")
    replay_fps = [fp for fp in (raw_fps if isinstance(raw_fps, list) else []) if isinstance(fp, str) and fp]
    legacy_fps = [fp for fp in (entry.get("legacy_fingerprints") if isinstance(entry.get("legacy_fingerprints"), list) else []) if isinstance(fp, str) and fp]
    cooldown_fp = _safe_str(entry.get("fingerprint"))
    has_legacy_fp_tomb = bool(cooldown_fp and isinstance(legacy_fps_map, dict) and cooldown_fp in legacy_fps_map)
    need_fp = (not action_id) or bool(entry.get("admitted_without_action_id")) or bool(replay_fps) or has_legacy_fp_tomb
    if need_fp:
        if payload and not replay_fps:
            variants = _row_fingerprint_variants(payload)
            for key_cand in card_keys:
                for var_payload, var_comment, var_list in variants:
                    replay_fps.append(relay_replay_fingerprint(key_cand, var_payload, var_comment, var_list))
        if cooldown_fp and not replay_fps and cooldown_fp not in legacy_fps:
            legacy_fps.append(cooldown_fp)
    tomb = {
        "record_id": str(u_key),
        "card_id": card_id,
        "short_card_id": short_card_id,
        "action_id": action_id,
        "action_id_inferred": action_id_inferred,
        "delivery_keys": delivery_keys,
        "fingerprints": _dedup_strs(replay_fps),
        "legacy_fingerprints": _dedup_strs(legacy_fps),
        "interrupted_at": float(entry.get("interrupted_at") or now),
    }
    if entry.get("admission_id"):
        tomb["admission_id"] = _safe_str(entry.get("admission_id"))
    if legacy_headerless:
        tomb["legacy_headerless"] = True
    if cooldown_fp:
        tomb["cooldown_fingerprint"] = cooldown_fp
    return tomb

def _ensure_unified_tombstones(state, now=None):
    now = time.time() if now is None else now
    dirty = False
    tombstones = state.get("unreconciled_tombstones")
    if not isinstance(tombstones, dict):
        tombstones = {}
        if "unreconciled_tombstones" in state:
            state["unreconciled_tombstones"] = tombstones
            dirty = True
    legacy_fps_map = state.get("unreconciled_fingerprints")
    u_cards = state.get("unreconciled_cards")
    if isinstance(u_cards, dict) and u_cards:
        if "unreconciled_tombstones" not in state:
            state["unreconciled_tombstones"] = tombstones
        for u_key, entry in u_cards.items():
            if not isinstance(entry, dict):
                continue
            key_str = str(u_key)
            if key_str not in tombstones:
                tombstones[key_str] = _build_tombstone_from_detail(
                    key_str,
                    entry,
                    state,
                    now,
                    legacy_fps_map=legacy_fps_map if isinstance(legacy_fps_map, dict) else None,
                )
                dirty = True
            elif not tombstones[key_str].get("record_id"):
                tombstones[key_str]["record_id"] = key_str
                dirty = True

    seen_actions = {t.get("action_id") for t in tombstones.values() if isinstance(t, dict) and t.get("action_id")}
    seen_fps = set()
    for t in tombstones.values():
        if isinstance(t, dict):
            seen_fps.update(t.get("fingerprints") or ())
            seen_fps.update(t.get("legacy_fingerprints") or ())

    legacy_actions = state.get("unreconciled_action_ids")
    if isinstance(legacy_actions, dict):
        if "unreconciled_tombstones" not in state and (legacy_actions or dirty):
            state["unreconciled_tombstones"] = tombstones
        for act_id, cid in legacy_actions.items():
            act_s = _safe_str(act_id)
            if act_s and act_s not in seen_actions:
                rec_id = f"legacy-action:{act_s}"
                idx = 1
                while rec_id in tombstones:
                    idx += 1
                    rec_id = f"legacy-action:{act_s}#{idx}"
                tombstones[rec_id] = {
                    "record_id": rec_id,
                    "card_id": str(normalize_canonical_trello_id(cid) or _safe_str(cid) or ""),
                    "short_card_id": None,
                    "action_id": act_s,
                    "action_id_inferred": False,
                    "delivery_keys": [],
                    "fingerprints": [],
                    "legacy_fingerprints": [],
                    "interrupted_at": now,
                }
                seen_actions.add(act_s)
        state.pop("unreconciled_action_ids", None)
        dirty = True

    if isinstance(legacy_fps_map, dict):
        if "unreconciled_tombstones" not in state and (legacy_fps_map or dirty):
            state["unreconciled_tombstones"] = tombstones
        for fp, meta in legacy_fps_map.items():
            fp_s = _safe_str(fp)
            if fp_s and fp_s not in seen_fps:
                rec_id = f"legacy-fp:{fp_s[:16]}"
                idx = 1
                while rec_id in tombstones:
                    idx += 1
                    rec_id = f"legacy-fp:{fp_s[:16]}#{idx}"
                cid = meta.get("card_id") if isinstance(meta, dict) else ""
                interrupted_ts = meta.get("interrupted_at") if isinstance(meta, dict) else now
                tombstones[rec_id] = {
                    "record_id": rec_id,
                    "card_id": str(normalize_canonical_trello_id(cid) or _safe_str(cid) or ""),
                    "short_card_id": None,
                    "action_id": None,
                    "action_id_inferred": False,
                    "delivery_keys": [],
                    "fingerprints": [],
                    "legacy_fingerprints": [fp_s],
                    "legacy_headerless": True,
                    "interrupted_at": float(interrupted_ts or now),
                }
                seen_fps.add(fp_s)
        state.pop("unreconciled_fingerprints", None)
        dirty = True
    return dirty

def _tombstone_matches(
    tombstone,
    action_id=None,
    action_id_inferred=False,
    delivery_keys=(),
    replay_fingerprints=(),
    legacy_fingerprints=(),
):
    if not isinstance(tombstone, dict):
        return False
    t_action = _safe_str(tombstone.get("action_id"))
    t_action_inferred = bool(tombstone.get("action_id_inferred"))
    t_dkeys = set(tombstone.get("delivery_keys") or ())
    in_dkeys = set(delivery_keys or ())
    if t_dkeys and in_dkeys:
        if t_dkeys & in_dkeys:
            return True
        if action_id and not action_id_inferred and t_action and not t_action_inferred:
            return action_id == t_action
        return False
    if action_id and t_action:
        if action_id == t_action:
            if not t_action_inferred or action_id_inferred:
                return True
            if t_dkeys and not in_dkeys:
                return False
        elif not action_id_inferred:
            return False
    if in_dkeys and not t_dkeys and not tombstone.get("legacy_headerless"):
        return False
    if t_dkeys and not in_dkeys and action_id and not action_id_inferred:
        return False
    t_fps = set(tombstone.get("fingerprints") or ())
    if t_fps and (t_fps & set(replay_fingerprints or ())):
        return True
    t_legacy = set(tombstone.get("legacy_fingerprints") or ())
    if t_legacy and (t_legacy & set(legacy_fingerprints or ())):
        return True
    return False

def is_quarantined_in_state(
    state,
    action_id=None,
    action_id_inferred=False,
    delivery_keys=(),
    replay_fingerprints=(),
    legacy_fingerprints=(),
):
    if not isinstance(state, dict):
        return False
    tombstones = state.get("unreconciled_tombstones")
    if isinstance(tombstones, dict):
        for tomb in tombstones.values():
            if _tombstone_matches(
                tomb,
                action_id=action_id,
                action_id_inferred=action_id_inferred,
                delivery_keys=delivery_keys,
                replay_fingerprints=replay_fingerprints,
                legacy_fingerprints=legacy_fingerprints,
            ):
                return True
    u_cards = state.get("unreconciled_cards")
    legacy_fps_map = state.get("unreconciled_fingerprints")
    if isinstance(u_cards, dict):
        for u_key, entry in u_cards.items():
            if not isinstance(entry, dict):
                continue
            if isinstance(tombstones, dict) and str(u_key) in tombstones:
                continue
            synth = _build_tombstone_from_detail(
                u_key,
                entry,
                state,
                0.0,
                legacy_fps_map=legacy_fps_map if isinstance(legacy_fps_map, dict) else None,
            )
            if _tombstone_matches(
                synth,
                action_id=action_id,
                action_id_inferred=action_id_inferred,
                delivery_keys=delivery_keys,
                replay_fingerprints=replay_fingerprints,
                legacy_fingerprints=legacy_fingerprints,
            ):
                return True
    if action_id:
        u_actions = state.get("unreconciled_action_ids")
        if isinstance(u_actions, dict) and action_id in u_actions:
            return True
    if isinstance(legacy_fps_map, dict):
        for fp in legacy_fingerprints or ():
            if fp in legacy_fps_map:
                return True
    return False

def _inferred_action_owned_by_other_delivery(state, action_id, delivery_keys):
    clean_act = _safe_str(action_id)
    in_dkeys = set(delivery_keys or ())
    if not isinstance(state, dict) or not clean_act or not in_dkeys:
        return False
    tombstones = state.get("unreconciled_tombstones")
    if isinstance(tombstones, dict):
        for tomb in tombstones.values():
            if isinstance(tomb, dict) and _safe_str(tomb.get("action_id")) == clean_act:
                t_dkeys = set(tomb.get("delivery_keys") or ())
                if t_dkeys and not (t_dkeys & in_dkeys):
                    return True
    for rt in state.get("recent_triggers") or ():
        if isinstance(rt, dict) and _safe_str(rt.get("id")) == clean_act:
            rt_dkeys = set(rt.get("delivery_keys") or ())
            if rt_dkeys and not (rt_dkeys & in_dkeys):
                return True
    return False

def _is_duplicate_recent_action(recent_triggers, action_id, action_id_inferred=False, delivery_keys=()):
    clean_act = _safe_str(action_id)
    if not clean_act or not isinstance(recent_triggers, list):
        return False
    in_dkeys = set(delivery_keys or ())
    for entry in recent_triggers:
        if not isinstance(entry, dict) or _safe_str(entry.get("id")) != clean_act:
            continue
        rt_inferred = bool(entry.get("action_id_inferred"))
        rt_dkeys = set(entry.get("delivery_keys") or ())
        if rt_dkeys and in_dkeys:
            if rt_dkeys & in_dkeys:
                return True
            if not rt_inferred and not action_id_inferred:
                return True
            continue
        if rt_inferred and not action_id_inferred:
            continue
        return True
    return False

def is_agent_or_suppressed_comment(comment, username):
    if username and username in SUPPRESSED_TRELLO_USERNAMES:
        return True
    if not comment:
        return False
    legacy_markers = ["[agy-sidecar:ack]", "[agy-sidecar:status]"]
    current_markers = [AGENT_ACK_MARKER, AGENT_STATUS_MARKER]
    all_markers = list(set(current_markers + legacy_markers))
    if any(marker and marker in comment for marker in all_markers):
        return True
    try:
        return bool(re.search(SUPPRESSED_COMMENT_REGEX, comment))
    except re.error as e:
        logging.warning("[Trello Sidecar] Invalid TRELLO_SUPPRESSED_COMMENT_REGEX: %s", e)
        return f"- Love {AGENT_SIGNATURE_NAME}" in comment

def should_accept_trigger(card_key, card_name, payload, comment, list_name, pre_enrichment=None):
    now = time.time()
    norm_key = str(normalize_canonical_trello_id(card_key) or card_key)
    username, display_name = extract_trigger_member(payload)
    if is_agent_or_suppressed_comment(comment, username):
        logging.info(
            "[Trello Sidecar] Ignoring trigger from suppressed member/signature on card '%s' (member=%s)",
            card_name,
            username or display_name,
        )
        log_trigger_ignored(norm_key, card_name, payload, comment, list_name, "suppressed_trigger_member")
        return False, "suppressed_trigger_member"
    if is_probable_agent_comment(comment, username, display_name):
        recent_comments = extract_recent_comment_texts(payload)
        if is_low_signal_agent_comment(comment, recent_comments):
            logging.info(
                "[Trello Sidecar] Ignoring low-novelty agent comment on card '%s' (member=%s).",
                card_name,
                username or display_name,
            )
            log_trigger_ignored(norm_key, card_name, payload, comment, list_name, "low_novelty_agent_comment")
            return False, "low_novelty_agent_comment"

    action_id = extract_action_id(payload)
    action_id_inferred = bool(pre_enrichment.get("action_id_inferred")) if isinstance(pre_enrichment, dict) and action_id else False
    fingerprint = trigger_fingerprint(norm_key, payload, comment, list_name)
    originated_without_action_id = (
        pre_enrichment.get("raw_action_id") is None
        if isinstance(pre_enrichment, dict) and "raw_action_id" in pre_enrichment
        else action_id is None
    )

    with state_lock:
        if norm_key in pending_cards:
            logging.info("[Trello Sidecar] Ignoring duplicate trigger while card '%s' already has a queued/running agent.", card_name)
            log_trigger_ignored(norm_key, card_name, payload, comment, list_name, "card_already_running")
            return False, "card_already_running"

        try:
            state = load_sidecar_state_strict()
        except SidecarStateReadError:
            logging.error("[Trello Sidecar] Failed to read trigger state for card '%s'; rejecting admission.", card_name)
            log_trigger_ignored(norm_key, card_name, payload, comment, list_name, "state_persistence_failed")
            return False, "state_persistence_failed"

        extra_shorts = []
        if isinstance(pre_enrichment, dict) and pre_enrichment.get("verified_short_id"):
            extra_shorts.append(pre_enrichment["verified_short_id"])
        verified_shorts = _verified_short_ids_for_card(state, norm_key, extra_verified_shorts=extra_shorts)
        delivery_keys = _compute_candidate_delivery_keys(
            norm_key,
            payload,
            pre_enrichment=pre_enrichment,
            verified_short_ids=verified_shorts,
        )
        if action_id and action_id_inferred and delivery_keys:
            if _inferred_action_owned_by_other_delivery(state, action_id, delivery_keys):
                action_id = None
                action_id_inferred = False
                if isinstance(payload, dict):
                    payload.pop("actionId", None)
        replay_fps, legacy_fps = _compute_candidate_fingerprints(
            norm_key,
            payload,
            comment,
            list_name,
            pre_enrichment=pre_enrichment,
            verified_short_ids=verified_shorts,
        )
        if is_quarantined_in_state(
            state,
            action_id=action_id,
            action_id_inferred=action_id_inferred,
            delivery_keys=delivery_keys,
            replay_fingerprints=replay_fps,
            legacy_fingerprints=legacy_fps,
        ):
            logging.warning(
                "[Trello Sidecar] Ignoring redelivery of unreconciled trigger (action=%s) for card '%s'.",
                action_id or "id-less",
                card_name,
            )
            log_trigger_ignored(norm_key, card_name, payload, comment, list_name, "unreconciled_action")
            return False, "unreconciled_action"

        in_flight = state.setdefault("in_flight_cards", {})
        if not isinstance(in_flight, dict):
            in_flight = {}
            state["in_flight_cards"] = in_flight
        for existing_key, existing_entry in list(in_flight.items()):
            existing_norm = str(normalize_canonical_trello_id(existing_key) or existing_key)
            if existing_norm != norm_key and str(existing_key).strip() not in verified_shorts:
                continue
            existing_stage = existing_entry.get("stage") if isinstance(existing_entry, dict) else None
            logging.error(
                "[Trello Sidecar] Refusing admission for card '%s': unowned residual in-flight row on disk (stage=%r).",
                card_name,
                existing_stage,
            )
            log_trigger_ignored(norm_key, card_name, payload, comment, list_name, "state_persistence_failed")
            return False, "state_persistence_failed"

        recent_triggers = state.setdefault("recent_triggers", [])
        if action_id and _is_duplicate_recent_action(
            recent_triggers,
            action_id,
            action_id_inferred=action_id_inferred,
            delivery_keys=delivery_keys,
        ):
            logging.info("[Trello Sidecar] Ignoring already-seen Trello action %s for card '%s'.", action_id, card_name)
            log_trigger_ignored(norm_key, card_name, payload, comment, list_name, "duplicate_action")
            return False, "duplicate_action"

        card_activity = state.setdefault("card_activity", {})
        last = card_activity.get(norm_key, {})
        last_seen = float(last.get("last_seen", 0) or 0)
        if last.get("fingerprint") == fingerprint and now - last_seen < TRIGGER_COOLDOWN_SECONDS:
            logging.info(
                "[Trello Sidecar] Ignoring repeated trigger fingerprint for card '%s' within %ss cooldown.",
                card_name,
                TRIGGER_COOLDOWN_SECONDS,
            )
            log_trigger_ignored(norm_key, card_name, payload, comment, list_name, "trigger_cooldown")
            return False, "trigger_cooldown"

        admission_id = secrets.token_hex(16)
        persisted_payload = copy.deepcopy(payload) if isinstance(payload, dict) else {}
        persisted_payload.pop("_admission_id", None)
        persisted_payload.pop("_defer_attempts", None)
        persisted_payload.pop("_action_id_inferred", None)
        if action_id:
            recent_triggers.append({
                "id": action_id,
                "card": norm_key,
                "seen_at": now,
                "admission_id": admission_id,
                "delivery_keys": delivery_keys,
                "action_id_inferred": action_id_inferred,
            })
            state["recent_triggers"] = recent_triggers[-MAX_RECENT_TRIGGERS:]
        card_activity[norm_key] = {
            "fingerprint": fingerprint,
            "last_seen": now,
            "last_action_id": action_id,
            "last_action_id_inferred": bool(action_id and action_id_inferred),
            "admission_id": admission_id,
        }
        row = {
            "card_name": card_name,
            "payload": persisted_payload,
            "queued_at": now,
            "stage": "queued",
            "admission_id": admission_id,
            "action_id": action_id,
            "action_id_inferred": bool(action_id and action_id_inferred),
            "delivery_keys": delivery_keys,
            "fingerprint": fingerprint,
            "replay_fingerprints": replay_fps if originated_without_action_id else [],
            "admitted_without_action_id": originated_without_action_id,
        }
        if verified_shorts:
            row["short_card_id"] = verified_shorts[0]
        in_flight[norm_key] = row
        pending_cards.add(norm_key)
        if not save_sidecar_state(state):
            pending_cards.discard(norm_key)
            logging.error("[Trello Sidecar] Failed to persist trigger state for card '%s'; rejecting admission.", card_name)
            log_trigger_ignored(norm_key, card_name, payload, comment, list_name, "state_persistence_failed")
            return False, "state_persistence_failed"
        if isinstance(payload, dict):
            payload["_admission_id"] = admission_id
            payload["_action_id_inferred"] = bool(action_id and action_id_inferred)

    log_trigger_accepted(norm_key, card_name, payload, comment, list_name)
    return True, "accepted"

def mark_in_flight_dispatched(
    card_key,
    conversation_id=None,
    admission_id=None,
    action_id=None,
    action_id_inferred=False,
    short_card_id=None,
    detail=None,
):
    key_str = str(normalize_canonical_trello_id(card_key) or card_key)
    clean_short = _safe_str(short_card_id)
    for attempt_save in range(2):
        retry_save = False
        with state_lock:
            try:
                state = load_sidecar_state_strict()
            except SidecarStateReadError:
                if attempt_save == 0:
                    time.sleep(0.02)
                    continue
                if isinstance(detail, dict):
                    detail["status"] = "transient_error"
                logging.error("[Trello Sidecar] Refusing responder launch: unreadable sidecar state for card '%s'.", key_str)
                return False
            in_flight = state.get("in_flight_cards")
            if not isinstance(in_flight, dict) or key_str not in in_flight:
                if action_id and is_quarantined_in_state(state, action_id=action_id, action_id_inferred=action_id_inferred):
                    if isinstance(detail, dict):
                        detail["status"] = "quarantined"
                    logging.warning(
                        "[Trello Sidecar] Refusing responder launch: action %s for card '%s' is quarantined.",
                        action_id,
                        key_str,
                    )
                    return False
                if isinstance(detail, dict):
                    detail["status"] = "mismatch"
                logging.error("[Trello Sidecar] Refusing responder launch: missing persisted admission row for card '%s'.", key_str)
                return False
            entry = in_flight[key_str]
            if not _matches_admission(entry, admission_id=admission_id, action_id=action_id):
                clean_action_id = _safe_str(action_id)
                entry_act = _safe_str(entry.get("action_id")) if isinstance(entry, dict) else None
                same_queued_adm = (
                    isinstance(entry, dict)
                    and entry.get("stage") == "queued"
                    and entry.get("admission_id") == admission_id
                    and bool(entry_act)
                    and bool(clean_action_id)
                    and entry_act != clean_action_id
                )
                if same_queued_adm:
                    now = time.time()
                    _ensure_unified_tombstones(state, now=now)
                    q_payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
                    q_name = _safe_str(entry.get("card_name")) or _safe_str(q_payload.get("cardName")) or "Unnamed Card"
                    q_act, q_inf, q_dkeys, q_rfps, _, q_short = _extract_row_quarantine_identities(
                        state,
                        key_str,
                        entry,
                        extra_shorts=[clean_short] if clean_short else (),
                    )
                    q_fp = _entry_fingerprint(key_str, entry)
                    q_conv = validate_conversation_id(conversation_id) or validate_conversation_id(entry.get("conversation_id"))
                    unreconciled = state.setdefault("unreconciled_cards", {})
                    if not isinstance(unreconciled, dict):
                        unreconciled = {}
                        state["unreconciled_cards"] = unreconciled
                    _record_quarantine_entry(
                        state,
                        unreconciled,
                        key_str,
                        q_name,
                        q_payload,
                        q_act,
                        entry.get("admission_id"),
                        q_fp,
                        q_conv,
                        "identity_conflict",
                        entry.get("dispatched_at"),
                        now,
                        save_fp=bool(entry.get("admitted_without_action_id") or not q_act),
                        replay_fingerprints=q_rfps,
                        short_card_id=q_short,
                        action_id_inferred=q_inf,
                        delivery_keys=q_dkeys,
                        legacy_headerless=_is_legacy_headerless_entry(entry, entry_delivery_keys=q_dkeys),
                    )
                    in_flight.pop(key_str, None)
                    if save_sidecar_state(state):
                        if isinstance(detail, dict):
                            detail["status"] = "identity_conflict"
                        logging.error(
                            "[Trello Sidecar] Quarantining card '%s' due to queued action identity conflict (%s != %s).",
                            key_str,
                            entry_act,
                            clean_action_id,
                        )
                        return False
                    retry_save = True
                else:
                    if isinstance(detail, dict):
                        detail["status"] = "mismatch"
                    logging.error(
                        "[Trello Sidecar] Refusing responder launch: mismatched/non-queued admission row for card '%s'.",
                        key_str,
                    )
                    return False
            if not retry_save:
                q_act, q_inf, q_dkeys, q_rfps, q_lfps, _ = _extract_row_quarantine_identities(
                    state,
                    key_str,
                    entry,
                    action_id=action_id,
                    action_id_inferred=action_id_inferred if action_id else None,
                    extra_shorts=[clean_short] if clean_short else (),
                )
                if is_quarantined_in_state(
                    state,
                    action_id=q_act,
                    action_id_inferred=q_inf,
                    delivery_keys=q_dkeys,
                    replay_fingerprints=q_rfps,
                    legacy_fingerprints=q_lfps,
                ):
                    if isinstance(detail, dict):
                        detail["status"] = "quarantined"
                    logging.warning(
                        "[Trello Sidecar] Refusing responder launch: quarantined trigger (action=%s) for card '%s'.",
                        q_act or "id-less",
                        key_str,
                    )
                    return False
                with process_cond:
                    if shutdown_event.is_set():
                        raise ShutdownRequested("Server shutting down")
                entry["stage"] = "dispatched"
                entry["dispatched_at"] = time.time()
                if clean_short:
                    entry["short_card_id"] = clean_short
                    raw_dk = extract_relay_delivery_key(entry.get("payload"))
                    if raw_dk:
                        entry["delivery_keys"] = _dedup_strs([
                            *(entry.get("delivery_keys") or ()),
                            f"{clean_short}:{raw_dk}",
                        ])
                if action_id and not entry.get("action_id"):
                    entry["action_id"] = action_id
                    entry["action_id_inferred"] = bool(action_id_inferred)
                    if isinstance(entry.get("payload"), dict):
                        entry["payload"]["actionId"] = action_id
                if conversation_id:
                    entry["conversation_id"] = conversation_id
                if save_sidecar_state(state):
                    if isinstance(detail, dict):
                        detail["status"] = "ok"
                    return True
        if attempt_save == 0:
            time.sleep(0.02)
    if isinstance(detail, dict):
        detail["status"] = "transient_error"
    logging.error("[Trello Sidecar] Failed to persist dispatched stage for card '%s'.", key_str)
    return False

def rollback_dispatched_to_queued(card_key, admission_id=None, max_attempts=3, retry_delay=0.05):
    key_str = str(normalize_canonical_trello_id(card_key) or card_key)
    for attempt in range(max(1, int(max_attempts))):
        with state_lock:
            try:
                state = load_sidecar_state_strict()
                read_ok = True
            except SidecarStateReadError:
                read_ok = False
                state = None
            if read_ok:
                in_flight = state.get("in_flight_cards")
                if not isinstance(in_flight, dict) or key_str not in in_flight:
                    return False
                entry = in_flight[key_str]
                if not isinstance(entry, dict):
                    return False
                if admission_id is not None and entry.get("admission_id") not in (None, admission_id):
                    return False
                if entry.get("stage") == "queued":
                    return True
                if entry.get("stage") != "dispatched":
                    return False
                entry["stage"] = "queued"
                entry.pop("dispatched_at", None)
                if save_sidecar_state(state):
                    return True
        if attempt + 1 < max_attempts and retry_delay > 0:
            time.sleep(retry_delay)
    logging.error("[Trello Sidecar] Failed to roll back dispatched stage to queued for card '%s'.", key_str)
    log_sidecar_event("dispatch_rollback_failed", {"card_id": key_str, "admission_id": admission_id})
    return False

def _expand_card_identifiers(state, raw_ids):
    expanded = set(_dedup_strs(raw_ids))
    aliases = state.get("card_aliases") if isinstance(state, dict) else {}
    for cid in list(expanded):
        norm_cid = normalize_canonical_trello_id(cid)
        if norm_cid:
            expanded.add(norm_cid)
        elif isinstance(aliases, dict):
            bridged = normalize_canonical_trello_id(aliases.get(cid))
            if bridged:
                expanded.add(bridged)
    for cid in list(expanded):
        norm_cid = normalize_canonical_trello_id(cid)
        if norm_cid:
            expanded.update(_verified_short_ids_for_card(state, norm_cid))
    return expanded

def _is_explicit_legacy_split_pair(
    state,
    tombstones,
    clean_rec,
    clean_act,
    clean_fp,
    act_matches,
    fp_matches,
    matched_ids,
):
    if clean_rec or not clean_act or not clean_fp:
        return False
    if len(act_matches) != 1 or len(fp_matches) != 1 or len(matched_ids) != 2:
        return False
    act_key = next(iter(act_matches))
    fp_key = next(iter(fp_matches))
    if not act_key.startswith("legacy-action:") or not fp_key.startswith("legacy-fp:"):
        return False
    if isinstance(tombstones, dict):
        t_act = tombstones.get(act_key)
        t_fp = tombstones.get(fp_key)
        if isinstance(t_act, dict) and isinstance(t_fp, dict):
            act_cards = _expand_card_identifiers(state, [t_act.get("card_id"), t_act.get("short_card_id")])
            fp_cards = _expand_card_identifiers(state, [t_fp.get("card_id"), t_fp.get("short_card_id")])
            if act_cards and fp_cards and not (act_cards & fp_cards):
                return False
    return True

def _reconcile_quarantined_entries(action_id=None, fingerprint=None, record_id=None):
    clean_rec = _safe_str(record_id)
    clean_act = _safe_str(action_id)
    clean_fp = _safe_str(fingerprint)
    if not clean_rec and not clean_act and not clean_fp:
        return "error", [], []
    with state_lock:
        try:
            state = load_sidecar_state_strict()
        except SidecarStateReadError:
            return "error", [], []
        dirty = _ensure_unified_tombstones(state)
        tombstones = state.get("unreconciled_tombstones")
        u_cards = state.get("unreconciled_cards")
        rec_matches = set()
        act_matches = set()
        fp_matches = set()

        if isinstance(tombstones, dict):
            for rec_key, tomb in list(tombstones.items()):
                if not isinstance(tomb, dict):
                    continue
                rk = str(rec_key)
                t_rec = _safe_str(tomb.get("record_id")) or rk
                t_act = _safe_str(tomb.get("action_id"))
                t_all_fps = set(tomb.get("fingerprints") or ()) | set(tomb.get("legacy_fingerprints") or ())
                if tomb.get("cooldown_fingerprint"):
                    t_all_fps.add(tomb["cooldown_fingerprint"])
                if clean_rec and (rk == clean_rec or t_rec == clean_rec):
                    rec_matches.add(rk)
                if clean_act and t_act == clean_act:
                    act_matches.add(rk)
                if clean_fp and clean_fp in t_all_fps:
                    fp_matches.add(rk)

        if isinstance(u_cards, dict):
            for k, entry in list(u_cards.items()):
                if not isinstance(entry, dict):
                    continue
                rk = str(k)
                e_act = _safe_str(entry.get("action_id"))
                e_all_fps = set(entry.get("replay_fingerprints") or ()) | set(entry.get("legacy_fingerprints") or ())
                if entry.get("fingerprint"):
                    e_all_fps.add(entry["fingerprint"])
                if clean_rec and rk == clean_rec:
                    rec_matches.add(rk)
                if clean_act and e_act == clean_act:
                    act_matches.add(rk)
                if clean_fp and clean_fp in e_all_fps:
                    fp_matches.add(rk)

        matched_ids = rec_matches | act_matches | fp_matches
        if len(matched_ids) > 1 and not _is_explicit_legacy_split_pair(
            state,
            tombstones,
            clean_rec,
            clean_act,
            clean_fp,
            act_matches,
            fp_matches,
            matched_ids,
        ):
            sorted_ambiguous = sorted(matched_ids)
            logging.warning(
                "[Trello Sidecar] Refusing ambiguous quarantine reconciliation matching %d records (%s).",
                len(sorted_ambiguous),
                ", ".join(sorted_ambiguous[:5]),
            )
            log_sidecar_event(
                "quarantine_reconcile_ambiguous",
                {"matching_record_ids": sorted_ambiguous[:20], "count": len(sorted_ambiguous)},
            )
            return "ambiguous", sorted_ambiguous, []

        cleared_actions = set()
        cleared_authoritative_actions = set()
        cleared_admission_ids = set()
        raw_affected_cards = set()
        for mid in matched_ids:
            for item in (
                tombstones.get(mid) if isinstance(tombstones, dict) else None,
                u_cards.get(mid) if isinstance(u_cards, dict) else None,
            ):
                if not isinstance(item, dict):
                    continue
                item_act = _safe_str(item.get("action_id"))
                item_adm = _safe_str(item.get("admission_id"))
                if item_act:
                    cleared_actions.add(item_act)
                    if not item.get("action_id_inferred"):
                        cleared_authoritative_actions.add(item_act)
                if item_adm:
                    cleared_admission_ids.add(item_adm)
                raw_affected_cards.update(_dedup_strs([item.get("card_id"), item.get("short_card_id")]))

        affected_cards = _expand_card_identifiers(state, raw_affected_cards)

        for mid in matched_ids:
            if isinstance(tombstones, dict) and tombstones.pop(mid, None) is not None:
                dirty = True
            if isinstance(u_cards, dict) and u_cards.pop(mid, None) is not None:
                dirty = True

        if (cleared_actions or cleared_admission_ids) and isinstance(state.get("recent_triggers"), list):
            prev_rt = state["recent_triggers"]
            new_rt = []
            for rt in prev_rt:
                if not isinstance(rt, dict):
                    new_rt.append(rt)
                    continue
                rt_adm = _safe_str(rt.get("admission_id"))
                rt_id = _safe_str(rt.get("id"))
                if rt_adm and rt_adm in cleared_admission_ids:
                    continue
                if rt_id and rt_id in cleared_actions and (not rt_adm or not cleared_admission_ids):
                    continue
                new_rt.append(rt)
            if len(new_rt) != len(prev_rt):
                state["recent_triggers"] = new_rt
                dirty = True

        card_activity = state.get("card_activity")
        if isinstance(card_activity, dict) and affected_cards and (cleared_admission_ids or cleared_authoritative_actions):
            for cid in affected_cards:
                ca = card_activity.get(cid)
                if not isinstance(ca, dict):
                    continue
                ca_adm = _safe_str(ca.get("admission_id"))
                ca_act = _safe_str(ca.get("last_action_id"))
                if ca_adm:
                    if ca_adm in cleared_admission_ids:
                        card_activity.pop(cid, None)
                        dirty = True
                elif ca_act and not ca.get("last_action_id_inferred") and ca_act in cleared_authoritative_actions:
                    card_activity.pop(cid, None)
                    dirty = True

        remaining_legacy_orphans = []
        if isinstance(tombstones, dict) and affected_cards:
            for rec_key, rem_tomb in tombstones.items():
                rec_s = str(rec_key)
                if not rec_s.startswith(("legacy-action:", "legacy-fp:")) or not isinstance(rem_tomb, dict):
                    continue
                rem_cards = _expand_card_identifiers(state, [rem_tomb.get("card_id"), rem_tomb.get("short_card_id")])
                if rem_cards & affected_cards:
                    remaining_legacy_orphans.append(rec_s)
        remaining_legacy_orphans.sort()

        if not dirty:
            return "ok", [], []
        if not save_sidecar_state(state):
            return "error", [], []
    sorted_ids = sorted(matched_ids)
    if sorted_ids:
        log_sidecar_event(
            "quarantine_reconciled",
            {
                "record_ids": sorted_ids[:20],
                "cleared": len(sorted_ids),
                "remaining_legacy_orphans": len(remaining_legacy_orphans),
            },
        )
    return "ok", sorted_ids, remaining_legacy_orphans

def reconcile_quarantined_entry(action_id=None, fingerprint=None, record_id=None):
    status, _, _ = _reconcile_quarantined_entries(
        action_id=action_id,
        fingerprint=fingerprint,
        record_id=record_id,
    )
    return status == "ok"

def release_pending_card(card_key, admission_id=None):
    key_str = str(normalize_canonical_trello_id(card_key) or card_key)
    with state_lock:
        try:
            state = load_sidecar_state_strict()
        except SidecarStateReadError:
            pending_cards.discard(card_key)
            pending_cards.discard(key_str)
            logging.error("[Trello Sidecar] Failed to read sidecar state while releasing card '%s'.", key_str)
            log_sidecar_event("in_flight_release_failed", {"card_id": key_str})
            return False
        in_flight = state.get("in_flight_cards", {})
        matching_keys = []
        mismatch = False
        if isinstance(in_flight, dict):
            for candidate_key in list(in_flight.keys()):
                candidate_norm = str(normalize_canonical_trello_id(candidate_key) or candidate_key)
                if candidate_norm != key_str and str(candidate_key) != str(card_key):
                    continue
                entry = in_flight.get(candidate_key)
                entry_adm = entry.get("admission_id") if isinstance(entry, dict) else None
                if admission_id is not None and entry_adm is not None and entry_adm != admission_id:
                    mismatch = True
                    continue
                matching_keys.append(candidate_key)
        if mismatch and not matching_keys:
            return True
        pending_cards.discard(card_key)
        pending_cards.discard(key_str)
        removed = False
        for candidate_key in matching_keys:
            if in_flight.pop(candidate_key, None) is not None:
                removed = True
        if removed:
            saved = save_sidecar_state(state)
            if not saved:
                time.sleep(0.02)
                saved = save_sidecar_state(state)
            if not saved:
                logging.error("[Trello Sidecar] Failed to persist in-flight removal for card '%s'.", key_str)
                log_sidecar_event("in_flight_release_failed", {"card_id": key_str})
                return False
    return True

def run_gh_json(args, cwd=None, timeout=12):
    gh_bin = shutil.which("gh")
    if not gh_bin:
        return None, "gh CLI is not installed or not on PATH"
    try:
        res = subprocess.run(
            [gh_bin, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )
    except Exception as e:
        return None, str(e)
    if res.returncode != 0:
        return None, res.stderr.strip() or res.stdout.strip()
    try:
        return json.loads(res.stdout or "[]"), None
    except json.JSONDecodeError as e:
        return None, f"Failed to parse gh JSON output: {e}"

def build_duplicate_search_queries(card_name, card_link):
    queries = []
    if card_link:
        queries.append(f'"{card_link}"')
        short_id = extract_short_id(card_link)
        if short_id:
            queries.append(short_id)
    title = normalize_trigger_text(card_name)
    title = re.sub(r"[^a-z0-9 ]+", " ", title)
    words = [w for w in title.split() if len(w) > 2][:8]
    if words:
        queries.append(" ".join(words))
    return list(dict.fromkeys(queries))

def collect_github_work_context(card_name, card_link):
    if not WORKSPACES:
        return ""

    repos = []
    for ws in WORKSPACES:
        repo = resolve_git_repo(ws)
        if repo and repo not in repos:
            repos.append(repo)
    if not repos:
        return ""

    lines = [
        "--- GitHub Duplicate/Progress Preflight ---",
        "The sidecar searched configured GitHub repositories before this planner run.",
        "You MUST treat these as blocking context before creating any issue.",
    ]
    found_any = False
    queries = build_duplicate_search_queries(card_name, card_link)

    for repo in repos:
        lines.append(f"\nRepository `{repo}`:")
        repo_found = False
        for query in queries:
            issue_data, issue_err = run_gh_json([
                "issue", "list",
                "--repo", repo,
                "--state", "all",
                "--limit", "10",
                "--search", query,
                "--json", "number,title,state,url,updatedAt,labels",
            ])
            pr_data, pr_err = run_gh_json([
                "pr", "list",
                "--repo", repo,
                "--state", "all",
                "--limit", "10",
                "--search", query,
                "--json", "number,title,state,url,updatedAt,isDraft",
            ])

            if issue_data:
                found_any = True
                repo_found = True
                lines.append(f"- Issues matching `{query}`:")
                for item in issue_data:
                    labels = ", ".join(label.get("name", "") for label in item.get("labels", []))
                    labels_str = f"; labels: {labels}" if labels else ""
                    lines.append(
                        f"  - #{item.get('number')} [{item.get('state')}] {item.get('title')} "
                        f"({item.get('url')}; updated {item.get('updatedAt')}{labels_str})"
                    )
            elif issue_err:
                lines.append(f"- Issue search `{query}` failed: {issue_err}")

            if pr_data:
                found_any = True
                repo_found = True
                lines.append(f"- PRs matching `{query}`:")
                for item in pr_data:
                    draft = "draft, " if item.get("isDraft") else ""
                    lines.append(
                        f"  - #{item.get('number')} [{draft}{item.get('state')}] {item.get('title')} "
                        f"({item.get('url')}; updated {item.get('updatedAt')})"
                    )
            elif pr_err:
                lines.append(f"- PR search `{query}` failed: {pr_err}")

        if not repo_found:
            lines.append("- No matching issues or PRs found by the sidecar preflight.")

    if not found_any:
        lines.append(
            "\nNo candidate duplicate/progress item was found automatically. You still MUST run your own targeted `gh issue list` "
            "and `gh pr list` searches before creating new issues."
        )
    lines.append("-------------------------------------------\n")
    return "\n".join(lines)

def resolve_conversation_alias(
    session_map,
    card_key,
    card_name,
    card_link=None,
    card_short_id=None,
    canonical_id=None,
    trusted_short_id=None,
):
    immutable_ids = []
    norm_canonical = normalize_canonical_trello_id(canonical_id) or canonical_id
    norm_key = normalize_canonical_trello_id(card_key) or card_key
    has_immutable_key = bool(
        norm_key
        and norm_key != "Unnamed Card"
        and (is_canonical_trello_id(norm_key) or norm_key != card_name)
    )
    if norm_canonical:
        immutable_ids.append(norm_canonical)
        if trusted_short_id and trusted_short_id not in immutable_ids:
            immutable_ids.append(trusted_short_id)
    elif has_immutable_key:
        immutable_ids.append(norm_key)
        if trusted_short_id and trusted_short_id not in immutable_ids:
            immutable_ids.append(trusted_short_id)
    else:
        for candidate in [trusted_short_id, card_short_id, extract_short_id(card_link)]:
            if candidate and candidate not in immutable_ids:
                immutable_ids.append(candidate)

    aliases = immutable_ids if immutable_ids else ([card_name] if card_name and card_name != "Unnamed Card" else [])
    canonical_alias = next((a for a in aliases if is_canonical_trello_id(a)), None)
    other_aliases = [a for a in aliases if a != canonical_alias]

    if canonical_alias:
        norm_alias = normalize_canonical_trello_id(canonical_alias)
        case_matches = {}
        for existing_key, existing_val in list(session_map.items()):
            if normalize_canonical_trello_id(existing_key) == norm_alias:
                cid = validate_conversation_id(existing_val)
                if cid:
                    case_matches.setdefault(cid, []).append(existing_key)
        if len(case_matches) > 1:
            log_sidecar_event(
                "session_alias_conflict",
                {
                    "card_id": norm_alias,
                    "matched_keys": [k for keys in case_matches.values() for k in keys][:10],
                },
            )
            return None, [], True
        if len(case_matches) == 1:
            selected_cid, matched_keys = next(iter(case_matches.items()))
            for mk in matched_keys:
                if mk != canonical_alias:
                    session_map.pop(mk, None)
            session_map[canonical_alias] = selected_cid
            writable_aliases = [canonical_alias]
            for other_alias in other_aliases:
                existing_other = validate_conversation_id(session_map.get(other_alias))
                if existing_other and existing_other != selected_cid:
                    log_sidecar_event(
                        "session_alias_conflict",
                        {
                            "card_id": norm_alias,
                            "matched_keys": [matched_keys[0], other_alias],
                        },
                    )
                else:
                    if other_alias and existing_other != selected_cid:
                        session_map[other_alias] = selected_cid
                    if other_alias and other_alias not in writable_aliases:
                        writable_aliases.append(other_alias)
            return selected_cid, writable_aliases, False

    lookup_aliases = other_aliases if canonical_alias else aliases
    other_matches = {}
    for alias in lookup_aliases:
        cid = validate_conversation_id(session_map.get(alias))
        if cid:
            other_matches.setdefault(cid, []).append(alias)
    if len(other_matches) > 1:
        log_sidecar_event(
            "session_alias_conflict",
            {
                "card_id": canonical_alias or norm_key,
                "matched_keys": [k for keys in other_matches.values() for k in keys][:10],
            },
        )
        return None, [], True
    if len(other_matches) == 1:
        found_cid, matched_other_keys = next(iter(other_matches.items()))
        writable_aliases = []
        for alias in aliases:
            existing_val = validate_conversation_id(session_map.get(alias))
            if existing_val and existing_val != found_cid:
                log_sidecar_event(
                    "session_alias_conflict",
                    {
                        "card_id": canonical_alias or norm_key,
                        "matched_keys": [matched_other_keys[0], alias],
                    },
                )
            else:
                if alias and existing_val != found_cid:
                    session_map[alias] = found_cid
                if alias and alias not in writable_aliases:
                    writable_aliases.append(alias)
        return found_cid, writable_aliases, False

    writable_fresh = [a for a in aliases if a and not validate_conversation_id(session_map.get(a))]
    return None, writable_fresh, False

def persist_conversation_id(aliases, new_id, expected_existing=None):
    cid = validate_conversation_id(new_id)
    safe_aliases = [a for a in (aliases or []) if a]
    if not cid or not safe_aliases:
        return False
    for attempt_save in range(2):
        with state_lock:
            try:
                latest_map = load_session_mapping_strict()
            except SessionMappingReadError:
                latest_map = None
            if latest_map is not None:
                wrote_any = False
                for alias in safe_aliases:
                    existing_cid = validate_conversation_id(latest_map.get(alias))
                    if existing_cid and existing_cid != cid and existing_cid != expected_existing:
                        log_sidecar_event(
                            "session_alias_conflict",
                            {
                                "card_id": alias,
                                "matched_keys": [alias],
                            },
                        )
                        continue
                    latest_map[alias] = cid
                    wrote_any = True
                if not wrote_any:
                    return False
                if save_session_mapping(latest_map):
                    return True
        if attempt_save == 0:
            time.sleep(0.02)
    return False

def collect_previous_conversation_context(conversation_id, max_chars=7000):
    cid = validate_conversation_id(conversation_id)
    if not cid:
        return ""
    brain_path = os.path.expanduser(f"~/.gemini/antigravity-cli/brain/{cid}")
    if not os.path.isdir(brain_path):
        return ""

    lines = [
        "--- Previous Agy Conversation Artifacts ---",
        f"Conversation ID: {conversation_id}",
        "Use this as historical context for the same Trello card. If prior artifacts show GitHub issues/PRs already created or decisions already made, do not recreate them; update/link the existing work instead.",
    ]
    used_chars = 0
    try:
        filenames = sorted(os.listdir(brain_path))
    except Exception as e:
        return f"--- Previous Agy Conversation Artifacts ---\nCould not read brain artifacts for {conversation_id}: {e}\n-------------------------------------------\n"

    for filename in filenames:
        if filename.endswith(".metadata.json"):
            path = os.path.join(brain_path, filename)
            try:
                with open(path, "r") as f:
                    metadata = json.load(f)
                summary = metadata.get("summary")
                if summary:
                    lines.append(f"- {filename}: {summary}")
            except Exception:
                continue

    for filename in filenames:
        if not filename.endswith(".md"):
            continue
        path = os.path.join(brain_path, filename)
        try:
            with open(path, "r") as f:
                content = f.read().strip()
        except Exception:
            continue
        if not content:
            continue
        remaining = max_chars - used_chars
        if remaining <= 0:
            break
        snippet = content[: min(remaining, 1800)]
        used_chars += len(snippet)
        if len(content) > len(snippet):
            snippet += "\n[truncated]"
        lines.append(f"\n### {filename}\n{snippet}")

    non_md_files = [filename for filename in filenames if not filename.endswith((".md", ".metadata.json"))]
    if non_md_files:
        lines.append("\nOther artifact files present but not inlined: " + ", ".join(non_md_files[:20]))

    lines.append("-------------------------------------------\n")
    return "\n".join(lines)

AUDIENCE_RULES = (
    "### Audience & Output Rules\n"
    "- Trello comments are for PMs, designers, QA, and reporters. Keep them plain-language, product-facing, and short.\n"
    "- Default Trello comment length: 3-6 bullets or under 120 words. Use more only when directly answering several explicit questions.\n"
    "- Do not include file paths, class names, API routes, database fields, implementation details, tool names, or command narration in Trello comments unless a human explicitly asks for technical detail.\n"
    "- Never explain which tools you considered or say you are about to run a command. Just perform the action and summarize the result.\n"
    "- For design-facing replies, discuss user flow, screen behavior, copy, layout, states, and decisions needed. Avoid backend or repo terminology.\n"
    "- Put technical details in GitHub issue bodies, not Trello comments. Trello should say what changed, what decision is needed, or where the GitHub issue/PR is.\n"
    "- If a comment asks a simple question about existing work, answer the question first. Do not re-open planning, create duplicate issues, or move cards unless explicitly requested.\n"
)

GITHUB_ISSUE_QUALITY_RULES = (
    "### GitHub Issue Quality Rules\n"
    "- Write issues for coding agents, not PMs. They must be specific enough to implement without re-reading the whole Trello thread.\n"
    "- Start every issue with a short Product Summary in plain language, then a Technical Implementation section.\n"
    "- Include source links: Trello card, related Trello cards, existing issues/PRs, screenshots/mockups, and relevant code paths found during grounding.\n"
    "- Clearly separate FE, BE, QA, analytics, and deployment/staging concerns.\n"
    "- Include exact acceptance criteria that an implementation agent can verify in browser/API tests.\n"
    "- If the work touches a post-merge follow-up, prefer creating/linking a new Trello card or GitHub issue instead of expanding an already-merged issue, unless the reporter is clearly adding context to active unfinished work.\n"
)

PROCESS_AUTOMATION_RULES = (
    "### Process Automation Rules\n"
    "- The pipeline goal is: Trello request discussion -> high-quality GitHub issue -> coding agent implementation -> PR -> staging feedback.\n"
    "- Preserve context, but do not let old cards become endless implementation threads.\n"
    "- If related PRs are already merged/deployed and the reporter asks for additional changes, treat it as follow-up work. Recommend or create/link a new Trello card/GitHub issue while referencing the original card for context.\n"
    "- If work is still active in an open issue/PR, update/link the active work instead of creating new duplicates.\n"
)

CODEX_REVIEW_PROMPT = (
    "Ask Codex for a bounded review, not a rewrite. Tell Codex to apply Superpowers-style discipline if available: "
    "brainstorming for unclear requirements, writing-plans for implementation specs, systematic-debugging for bug work, "
    "test-driven-development for implementation guidance, and verification-before-completion for completion criteria. "
    "The prompt MUST include: "
    "(1) one-paragraph product goal, (2) user-facing behavior, (3) repo/code paths inspected, "
    "(4) proposed FE/BE split, (5) acceptance criteria, (6) known open questions, and (7) duplicate/progress findings. "
    "Ask Codex to return only: top 5 risks, missing product decisions, likely duplicate/related work, test gaps, and concrete edits to the issue spec. "
    "Tell Codex not to restate the whole plan."
)

SUPERPOWERS_RULES = (
    "### Superpowers Workflow Rules\n"
    "- If the Superpowers skill/plugin is available, use it for process discipline.\n"
    "- For unclear or creative product work, use brainstorming before planning or implementation-oriented recommendations.\n"
    "- For bug investigation, use systematic-debugging and identify evidence/root cause before proposing fixes.\n"
    "- For Ready for Spec work, use writing-plans style structure: clear goal, architecture, files/surfaces, implementation tasks, tests, and verification.\n"
    "- For implementation handoff, include TDD-minded acceptance criteria and verification-before-completion checks.\n"
    "- Do not expose Superpowers/tool ceremony in Trello comments; apply the workflow silently and summarize only useful outcomes.\n"
)

def format_user_profile(profile, prefix="- "):
    labels = [
        ("display_name", "Display name"),
        ("trello_username", "Trello username"),
        ("role", "Role"),
        ("authority", "Authority"),
        ("preferred_address", "Preferred address"),
        ("mention_policy", "Mention policy"),
        ("tone", "Tone"),
        ("notes", "Notes"),
    ]
    lines = []
    for key, label in labels:
        value = profile.get(key)
        if not value:
            continue
        if key == "trello_username":
            value = f"@{value}"
        lines.append(f"{prefix}{label}: {value}")
    return "\n".join(lines)

def build_stakeholder_context_block(trigger_username):
    if not STAKEHOLDER_CONTEXT.get("users") and not STAKEHOLDER_CONTEXT.get("rules"):
        return ""

    trigger_key = normalize_roster_key(trigger_username)
    trigger_profile = STAKEHOLDER_CONTEXT.get("users_by_username", {}).get(trigger_key)
    lines = [
        "--- Stakeholder Context ---",
        "This deployment provided local stakeholder context. Use it to adapt tone, authority, escalation, and mention behavior. Do not invent roles for people who are not listed.",
        "Mention policy meanings: `never_at_mention` means address by preferred name without @; `direct_reply_only` means @mention only when directly replying to that person; `ok` means normal Trello mention behavior is allowed.",
    ]

    if trigger_profile:
        lines.append("\nTriggering user profile:")
        lines.append(format_user_profile(trigger_profile))
    elif trigger_username:
        lines.append(f"\nNo roster profile found for triggering username @{normalize_roster_key(trigger_username)}. Use default PM/designer-friendly tone unless the card context says otherwise.")

    roster_lines = []
    for profile in STAKEHOLDER_CONTEXT.get("users", [])[:40]:
        name = profile.get("display_name") or profile.get("trello_username") or "Unknown"
        username = f" (@{profile['trello_username']})" if profile.get("trello_username") else ""
        role = f" - {profile['role']}" if profile.get("role") else ""
        authority = f"; authority: {profile['authority']}" if profile.get("authority") else ""
        mention = f"; mention: {profile['mention_policy']}" if profile.get("mention_policy") else ""
        tone = f"; tone: {profile['tone']}" if profile.get("tone") else ""
        roster_lines.append(f"- {name}{username}{role}{authority}{mention}{tone}")
    if roster_lines:
        lines.append("\nConfigured roster:")
        lines.extend(roster_lines)

    rules = STAKEHOLDER_CONTEXT.get("rules", [])
    if rules:
        lines.append("\nDeployment-specific rules:")
        lines.extend(f"- {rule}" for rule in rules)

    lines.append("---------------------------\n")
    return "\n".join(lines)

def build_trello_helper_rules(helper_path=TRELLO_HELPER_PATH):
    quoted_path = shlex.quote(str(helper_path))
    return (
        "### Trello Helper Utility (MANDATORY)\n"
        "You MUST use the pre-installed CLI utility via run_command for ALL Trello operations (comment, move, add-label, remove-label). Do NOT construct raw `curl` commands, do NOT use inline HTTP request scripts, and do NOT write custom python files for Trello API calls. You must invoke the helper exactly as follows:\n"
        f"- Comment: `python3 {quoted_path} comment <card_id> \"<text>\"`\n"
        f"- Move to List: `python3 {quoted_path} move <card_id> \"<list_name>\"`\n"
        f"- Add Label: `python3 {quoted_path} add-label <card_id> \"<label_name>\"`\n"
        f"- Remove Label: `python3 {quoted_path} remove-label <card_id> \"<label_name>\"`\n"
    )

TRELLO_HELPER_RULES = build_trello_helper_rules()

def normalize_comment_for_match(text):
    normalized = re.sub(r"@\*\*([A-Za-z0-9_]+)\*\*", r"@\1", str(text or ""))
    return re.sub(r"\s+", " ", normalized).strip().lower()

def resolve_comment_match(card_data, comment_text=None, action_id=None, author_username=None):
    if not isinstance(card_data, dict):
        return None, None
    comment_actions = [
        a for a in (card_data.get("actions") or [])
        if isinstance(a, dict) and a.get("type") == "commentCard"
    ]
    if not comment_actions:
        return None, None
    if action_id:
        for action in comment_actions:
            if action.get("id") == action_id:
                return action, action_id
        return None, None
    target = normalize_comment_for_match(comment_text)
    if not target:
        return None, None
    matches = [
        action for action in comment_actions
        if normalize_comment_for_match((action.get("data") or {}).get("text", "")) == target
    ]
    author_norm = (_safe_str(author_username) or "").lower()
    if author_norm:
        matches = [
            action for action in matches
            if ((action.get("memberCreator") or {}).get("username") or "").strip().lower() == author_norm
        ]
    if not matches:
        return None, None
    matches.sort(key=lambda x: x.get("date", ""), reverse=True)
    authors = {
        ((m.get("memberCreator") or {}).get("username") or "").strip().lower()
        for m in matches
    }
    authors.discard("")
    if len(authors) > 1:
        return None, None
    unique_aid = _safe_str(matches[0].get("id")) if len(matches) == 1 else None
    return matches[0], unique_aid

def resolve_comment_action(card_data, comment_text=None, action_id=None, author_username=None):
    matched, _ = resolve_comment_match(
        card_data,
        comment_text=comment_text,
        action_id=action_id,
        author_username=author_username,
    )
    return matched

def find_matching_comment_action(card_data, comment_text):
    return resolve_comment_action(card_data, comment_text=comment_text)

def verify_auth_token(incoming_token, expected_token):
    if not incoming_token or not expected_token:
        return False
    candidate = str(incoming_token).strip()
    if candidate.lower().startswith("bearer "):
        candidate = candidate[7:].strip()
    if not candidate:
        return False
    return secrets.compare_digest(candidate.encode("utf-8"), str(expected_token).encode("utf-8"))

def verify_trello_signature(raw_body_bytes, header_sig, callback_url, secret):
    if not raw_body_bytes or not header_sig or not callback_url or not secret:
        return False
    digest = hmac.new(
        str(secret).encode("utf-8"),
        raw_body_bytes + str(callback_url).encode("utf-8"),
        hashlib.sha1,
    ).digest()
    expected_sig = base64.b64encode(digest).decode("ascii")
    return secrets.compare_digest(
        str(header_sig).strip().encode("utf-8"),
        expected_sig.encode("utf-8"),
    )

def _serialize_summary(summary_dict, compact=False):
    filtered = {k: v for k, v in summary_dict.items() if v is not None}
    if compact:
        return json.dumps(filtered, separators=(",", ":"))
    return json.dumps(filtered, indent=2)

def build_compact_trigger_summary(
    payload,
    card_id=None,
    card_name=None,
    card_link=None,
    card_short_id=None,
    list_name=None,
    comment=None,
    trigger_username=None,
    trigger_name=None,
    max_comment_bytes=8192,
    max_summary_bytes=None,
):
    payload = payload if isinstance(payload, dict) else {}
    resolved_id = (
        normalize_canonical_trello_id(card_id)
        or card_id
        or normalize_canonical_trello_id(payload.get("cardId"))
        or payload.get("cardId")
        or normalize_canonical_trello_id(payload.get("cardidLong"))
        or payload.get("cardidLong")
        or normalize_canonical_trello_id(payload.get("cardIdLong"))
        or payload.get("cardIdLong")
    )
    raw_link = card_link if card_link is not None else payload.get("cardLink")
    resolved_short = (
        card_short_id
        or payload.get("cardShortId")
        or (extract_short_id(raw_link) if raw_link else None)
    )
    raw_comment = comment if comment is not None else payload.get("comment", "")
    raw_action_id = extract_action_id(payload)
    summary_fields = {
        "cardId": truncate_text_to_bytes(resolved_id, 64) if resolved_id else None,
        "cardShortId": truncate_text_to_bytes(resolved_short, 32) if resolved_short else None,
        "cardName": truncate_text_to_bytes(card_name or payload.get("cardName") or "", 256) or None,
        "cardLink": truncate_text_to_bytes(raw_link or "", 256) or None if raw_link else None,
        "listName": truncate_text_to_bytes(list_name or payload.get("listName") or "", 128) or None,
        "actionType": truncate_text_to_bytes(extract_action_type(payload), 64),
        "actionId": truncate_text_to_bytes(raw_action_id, 128) if raw_action_id else None,
        "triggerUserUsername": truncate_text_to_bytes(trigger_username or payload.get("triggerUserUsername") or "", 128) or None,
        "triggerUserName": truncate_text_to_bytes(trigger_name or payload.get("triggerUserName") or "", 128) or None,
        "comment": truncate_text_to_bytes(raw_comment, max_comment_bytes) if raw_comment else "",
    }
    serialized = _serialize_summary(summary_fields, compact=False)
    if max_summary_bytes is None or len(serialized.encode("utf-8")) <= max_summary_bytes:
        return serialized

    work = dict(summary_fields)
    for field_name in (
        "triggerUserName",
        "triggerUserUsername",
        "actionId",
        "cardName",
        "listName",
        "actionType",
    ):
        if work.pop(field_name, None) is not None:
            candidate = _serialize_summary(work, compact=False)
            if len(candidate.encode("utf-8")) <= max_summary_bytes:
                return candidate

    comment_text = str(work.get("comment") or "")
    for use_compact in (False, True):
        empty_try = _serialize_summary(dict(work, comment=""), compact=use_compact)
        if len(empty_try.encode("utf-8")) > max_summary_bytes:
            continue
        best = empty_try
        lo, hi = 0, len(comment_text.encode("utf-8"))
        while lo <= hi:
            mid = (lo + hi) // 2
            cand = _serialize_summary(
                dict(work, comment=truncate_text_to_bytes(comment_text, mid)),
                compact=use_compact,
            )
            if len(cand.encode("utf-8")) <= max_summary_bytes:
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    raise ValueError(
        f"Trigger summary budget ({max_summary_bytes} bytes) too small to preserve verified card identity"
    )

def build_agent_prompt(
    system_instruction,
    security_rules,
    payload_str,
    trigger_user_info="",
    stakeholder_context="",
    workspaces_info="",
    github_work_context="",
    previous_conversation_context="",
    card_details_str="",
    trigger_summary="",
    max_bytes=MAX_PROMPT_BYTES,
):
    role_block = f"Role & Core Instructions:\n{system_instruction}\n\n"
    security_block = f"Security Rules:\n{security_rules}\n\n"
    meta_head = ""
    if trigger_user_info:
        meta_head += trigger_user_info
    if workspaces_info:
        meta_head += f"--- Active Workspaces ---\n{workspaces_info}\n-------------------------\n\n"

    closing_instructions = (
        "1. Examine the Trello trigger summary, webhook payload, and live card state above to understand the current Trello card context (cardName, cardDescription, listName, checklists, comments, and recent activities).\n"
        "2. Execute your specialized mode behavior (PLANNER, INVESTIGATOR, or GENERAL discussion) based on the trigger and card state.\n"
        "3. Output your response or spec summary."
    )
    has_summary_source = bool(trigger_summary or payload_str)
    summary_wrapper_bytes = len("--- Trigger Summary ---\n\n-----------------------\n\n".encode("utf-8"))
    required_fixed = len((role_block + security_block + closing_instructions).encode("utf-8"))
    min_required = required_fixed + ((summary_wrapper_bytes + 64) if has_summary_source else 0)
    if min_required > max_bytes:
        raise ValueError(
            f"Agent prompt budget ({max_bytes} bytes) is smaller than required instructions/identity ({min_required} bytes)"
        )

    avail_for_summary = max_bytes - required_fixed - summary_wrapper_bytes
    summary_budget = min(max(max_bytes // 3, 256), avail_for_summary)

    if not trigger_summary and payload_str:
        try:
            parsed_payload = json.loads(payload_str)
        except Exception:
            parsed_payload = None
        if isinstance(parsed_payload, dict):
            trigger_summary = build_compact_trigger_summary(
                parsed_payload,
                max_comment_bytes=min(8192, max(128, summary_budget // 2)),
                max_summary_bytes=summary_budget,
            )
    elif trigger_summary and len(trigger_summary.encode("utf-8")) > summary_budget:
        try:
            parsed_summary = json.loads(trigger_summary)
        except Exception:
            parsed_summary = None
        if isinstance(parsed_summary, dict):
            trigger_summary = build_compact_trigger_summary(
                parsed_summary,
                max_comment_bytes=min(8192, max(128, summary_budget // 2)),
                max_summary_bytes=summary_budget,
            )
        else:
            trigger_summary = truncate_text_to_bytes(trigger_summary, summary_budget)

    trigger_core = (
        f"--- Trigger Summary ---\n{trigger_summary}\n-----------------------\n\n"
        if trigger_summary
        else ""
    )
    essential_tail = trigger_core + closing_instructions
    base_prompt = role_block + security_block
    fixed_bytes = len((base_prompt + essential_tail).encode("utf-8"))
    if fixed_bytes > max_bytes:
        raise ValueError(
            f"Agent prompt budget ({max_bytes} bytes) exceeded by required sections ({fixed_bytes} bytes)"
        )
    remaining_budget = max_bytes - fixed_bytes
    optional_parts = []
    payload_block = (
        f"--- Webhook Trigger Payload ---\n{payload_str}\n-------------------------------\n\n"
        if payload_str
        else ""
    )
    candidates = [
        meta_head,
        stakeholder_context,
        github_work_context,
        f"--- Trello Card Live State ---\n{card_details_str}\n------------------------------\n\n" if card_details_str else "",
        payload_block,
        previous_conversation_context,
    ]
    for section in candidates:
        if not section or remaining_budget <= 0:
            continue
        section_bytes = len(section.encode("utf-8"))
        if section_bytes <= remaining_budget:
            optional_parts.append(section)
            remaining_budget -= section_bytes
        elif remaining_budget > 256:
            trimmed = truncate_text_to_bytes(section, remaining_budget - 1)
            optional_parts.append(trimmed + "\n")
            remaining_budget = 0
            break

    return base_prompt + "".join(optional_parts) + essential_tail

def _strip_non_comment_text(payload):
    if not isinstance(payload, dict):
        return
    payload.pop("comment", None)
    action_obj = payload.get("action")
    if isinstance(action_obj, dict) and isinstance(action_obj.get("data"), dict):
        action_obj["data"].pop("text", None)

def _project_verified_card_identity(payload, canonical_id=None, verified_short_id=None, verified_url=None):
    if not isinstance(payload, dict):
        return
    norm_canonical = normalize_canonical_trello_id(canonical_id) or (
        str(canonical_id).strip() if isinstance(canonical_id, str) and canonical_id.strip() else None
    )
    short_val = str(verified_short_id).strip() if isinstance(verified_short_id, str) and verified_short_id.strip() else None
    url_val = str(verified_url).strip() if isinstance(verified_url, str) and verified_url.strip() else None

    for top_stale_key in ("shortUrl", "shortLink", "url", "cardidLong", "cardIdLong"):
        payload.pop(top_stale_key, None)
    if norm_canonical:
        payload["cardId"] = norm_canonical
    if url_val:
        payload["cardLink"] = url_val
    else:
        payload.pop("cardLink", None)
    if short_val:
        payload["cardShortId"] = short_val
    else:
        payload.pop("cardShortId", None)

    card_containers = []
    action_obj = payload.get("action")
    if isinstance(action_obj, dict) and isinstance(action_obj.get("data"), dict):
        card_obj = action_obj["data"].get("card")
        if isinstance(card_obj, dict):
            card_containers.append((card_obj, True))
    model_obj = payload.get("model")
    if isinstance(model_obj, dict):
        m_url = str(model_obj.get("url") or model_obj.get("shortUrl") or "")
        is_board_model = ("/b/" in m_url) and ("/c/" not in m_url)
        if not is_board_model:
            model_id_norm = normalize_canonical_trello_id(model_obj.get("id"))
            if model_id_norm and norm_canonical and model_id_norm != norm_canonical:
                for stale_k in ("id", "shortLink", "shortUrl", "url"):
                    model_obj.pop(stale_k, None)
            else:
                card_containers.append((model_obj, False))

    for container, force_id in card_containers:
        if norm_canonical and (force_id or container.get("id")):
            container["id"] = norm_canonical
        if short_val:
            container["shortLink"] = short_val
        else:
            container.pop("shortLink", None)
        if url_val:
            container["shortUrl"] = url_val
            container.pop("url", None)
        else:
            container.pop("shortUrl", None)
            container.pop("url", None)

def project_prompt_payload(
    payload,
    canonical_id=None,
    verified_short_id=None,
    verified_url=None,
    card_name=None,
    list_name=None,
    comment=None,
    action_type=None,
    action_id=None,
    trigger_username=None,
    trigger_name=None,
):
    payload = payload if isinstance(payload, dict) else {}
    resolved_id = (
        normalize_canonical_trello_id(canonical_id)
        or _safe_str(canonical_id)
        or normalize_canonical_trello_id(payload.get("cardId"))
        or _safe_str(payload.get("cardId"))
    )
    projected = {"cardId": resolved_id}
    safe_short = _safe_str(verified_short_id)
    if safe_short:
        projected["cardShortId"] = safe_short
    safe_url = _safe_str(verified_url)
    if safe_url:
        projected["cardLink"] = safe_url
    safe_card_name = _safe_str(card_name)
    if safe_card_name:
        projected["cardName"] = safe_card_name
    safe_list_name = _safe_str(list_name)
    if safe_list_name:
        projected["listName"] = safe_list_name
    eff_action_type = _safe_str(action_type) or extract_action_type(payload)
    if eff_action_type:
        projected["actionType"] = eff_action_type
    safe_action_id = _safe_str(action_id)
    if safe_action_id:
        projected["actionId"] = safe_action_id
    safe_user = _safe_str(trigger_username)
    if safe_user:
        projected["triggerUserUsername"] = safe_user
    safe_user_name = _safe_str(trigger_name)
    if safe_user_name:
        projected["triggerUserName"] = safe_user_name
    if eff_action_type not in NON_COMMENT_ACTION_TYPES and isinstance(comment, str) and comment.strip():
        projected["comment"] = comment

    action_obj = extract_action(payload)
    action_data = action_obj.get("data") if isinstance(action_obj.get("data"), dict) else {}
    board_info = None
    board_obj = action_data.get("board")
    if isinstance(board_obj, dict):
        b_proj = {}
        for k in ("id", "name", "shortLink"):
            v = _safe_str(board_obj.get(k))
            if v:
                b_proj[k] = v
        if b_proj:
            board_info = b_proj
    else:
        model_obj = payload.get("model")
        if isinstance(model_obj, dict):
            m_url = _safe_str(model_obj.get("url")) or _safe_str(model_obj.get("shortUrl")) or ""
            if "/b/" in m_url and "/c/" not in m_url:
                b_proj = {}
                for k in ("id", "name", "shortLink"):
                    v = _safe_str(model_obj.get(k))
                    if v:
                        b_proj[k] = v
                if b_proj:
                    board_info = b_proj
    if board_info:
        projected["board"] = board_info

    for field in ("listBefore", "listAfter", "label"):
        raw_val = action_data.get(field)
        resolved = _safe_str(raw_val.get("name")) if isinstance(raw_val, dict) else _safe_str(raw_val)
        if not resolved:
            resolved = _safe_str(payload.get(field))
        if resolved:
            projected[field] = resolved
    return projected

def _clear_owned_admission_from_state(state, key_str, entry, admission_id=None, action_id=None):
    in_flight = state.get("in_flight_cards")
    if isinstance(in_flight, dict):
        in_flight.pop(key_str, None)
    entry_action = _safe_str(entry.get("action_id")) if isinstance(entry, dict) else None
    action_ids_to_clear = {aid for aid in (entry_action, _safe_str(action_id)) if aid}
    eff_adm = _safe_str(admission_id) or (_safe_str(entry.get("admission_id")) if isinstance(entry, dict) else None)
    if (action_ids_to_clear or eff_adm) and isinstance(state.get("recent_triggers"), list):
        state["recent_triggers"] = [
            rt for rt in state["recent_triggers"]
            if not (
                isinstance(rt, dict)
                and (
                    (eff_adm and _safe_str(rt.get("admission_id")) == eff_adm)
                    or (
                        _safe_str(rt.get("id")) in action_ids_to_clear
                        and (not _safe_str(rt.get("admission_id")) or not eff_adm)
                    )
                )
            )
        ]
    card_activity = state.get("card_activity")
    if isinstance(card_activity, dict):
        ca = card_activity.get(key_str)
        if isinstance(ca, dict):
            ca_adm = _safe_str(ca.get("admission_id"))
            ca_act = _safe_str(ca.get("last_action_id"))
            owned = (
                (eff_adm and ca_adm == eff_adm)
                or (not ca_adm and not ca_act)
                or (not ca_adm and ca_act in action_ids_to_clear)
            )
            if owned:
                card_activity.pop(key_str, None)

def _release_undispatched_admission_with_dedup(card_key, admission_id=None, action_id=None):
    key_str = str(normalize_canonical_trello_id(card_key) or card_key)
    for attempt_save in range(2):
        with state_lock:
            try:
                state = load_sidecar_state_strict()
            except SidecarStateReadError:
                if attempt_save == 0:
                    time.sleep(0.02)
                    continue
                return False
            in_flight = state.get("in_flight_cards")
            entry = in_flight.get(key_str) if isinstance(in_flight, dict) else None
            if not isinstance(entry, dict) or not _matches_admission(entry, admission_id=admission_id, action_id=action_id):
                pending_cards.discard(card_key)
                pending_cards.discard(key_str)
                return True
            _clear_owned_admission_from_state(
                state,
                key_str,
                entry,
                admission_id=admission_id,
                action_id=action_id,
            )
            if save_sidecar_state(state):
                pending_cards.discard(card_key)
                pending_cards.discard(key_str)
                return True
        if attempt_save == 0:
            time.sleep(0.02)
    return False

def _defer_or_exhaust_queued_admission(
    card_key,
    card_name,
    payload,
    phase,
    list_name,
    action_id=None,
    admission_id=None,
    reason="unverified_card_link",
    local_defer_attempts=0,
):
    defer_attempt = max(1, int(local_defer_attempts or 0) + 1)
    defer_terminal = False
    defer_state_ok = True
    defer_lost_admission = False
    with state_lock:
        try:
            state = load_sidecar_state_strict()
            in_flight = state.get("in_flight_cards")
            entry = in_flight.get(card_key) if isinstance(in_flight, dict) else None
            if not _matches_admission(entry, admission_id=admission_id, action_id=action_id):
                defer_lost_admission = True
                defer_terminal = True
                defer_state_ok = False
            else:
                prev_disk = int(entry.get("defer_attempts") or 0)
                prev_attempts = max(prev_disk, int(local_defer_attempts or 0))
                defer_attempt = prev_attempts + 1
                if prev_attempts < DEFERRED_RETRY_MAX_ATTEMPTS:
                    entry["defer_attempts"] = defer_attempt
                    entry["last_deferred_at"] = time.time()
                    defer_state_ok = save_sidecar_state(state)
                else:
                    _clear_owned_admission_from_state(
                        state,
                        card_key,
                        entry,
                        admission_id=admission_id,
                        action_id=action_id,
                    )
                    defer_state_ok = save_sidecar_state(state)
                    if defer_state_ok:
                        defer_terminal = True
                        pending_cards.discard(card_key)
        except SidecarStateReadError:
            defer_state_ok = False
    if defer_lost_admission:
        logging.error(
            "[Trello Sidecar] Aborting deferred %s run for card '%s': missing or mismatched admission.",
            phase,
            card_name,
        )
        log_sidecar_event("trigger_deferred", {
            "card_id": card_key,
            "card_name": card_name,
            "reason": "missing_or_mismatched_admission",
            "phase": phase,
            "list_name": list_name,
            "action_id": extract_action_id(payload),
            "terminal": True,
            "state_saved": False,
        })
        raise RuntimeError("Deferred trigger lost matching queued admission")
    logging.warning(
        "[Trello Sidecar] Deferring %s run for card '%s': %s (attempt=%d, terminal=%s).",
        phase,
        card_name,
        reason,
        defer_attempt,
        defer_terminal,
    )
    log_sidecar_event("trigger_deferred", {
        "card_id": card_key,
        "card_name": card_name,
        "reason": reason,
        "phase": phase,
        "list_name": list_name,
        "action_id": extract_action_id(payload),
        "attempt": defer_attempt,
        "terminal": defer_terminal,
        "state_saved": defer_state_ok,
    })
    if not defer_terminal:
        raise DeferredTriggerRetry(DEFERRED_RETRY_DELAY_SECONDS, defer_attempts=defer_attempt)
    return True

async def trigger_agent(card_key, card_name, payload):
    """Spawns an Antigravity agent by executing the agy CLI binary as a subprocess."""
    payload = copy.deepcopy(payload) if isinstance(payload, dict) else {}
    admission_id = payload.pop("_admission_id", None)
    local_defer_attempts = int(payload.pop("_defer_attempts", 0) or 0)
    action_id_inferred = bool(payload.pop("_action_id_inferred", False))
    card_key = str(normalize_canonical_trello_id(card_key) or card_key)
    action_type = extract_action_type(payload)
    action_id = extract_action_id(payload)
    if not action_id:
        action_id_inferred = False
    if action_type in NON_COMMENT_ACTION_TYPES:
        comment = ""
        _strip_non_comment_text(payload)
    else:
        raw_comment = payload.get("comment", "")
        comment = raw_comment if isinstance(raw_comment, str) else ""
    comment_from_payload = bool(comment.strip())
    raw_desc = payload.get("cardDescription", "")
    card_desc = raw_desc if isinstance(raw_desc, str) else ""
    list_name = _safe_str(payload.get("listName")) or ""
    card_name = _safe_str(card_name) or "Unnamed Card"

    card_id = (
        normalize_canonical_trello_id(payload.get("cardId"))
        or normalize_canonical_trello_id(payload.get("cardidLong"))
        or normalize_canonical_trello_id(payload.get("cardIdLong"))
        or _safe_str(payload.get("cardId"))
    )
    if not card_id and _safe_str(payload.get("cardLink")):
        card_id = extract_short_id(_safe_str(payload.get("cardLink")))
        if card_id:
            payload["cardId"] = card_id
    if card_id:
        payload["cardId"] = card_id

    card_details_str = ""
    card_data = None
    canonical_card_id = None
    trusted_short_id = None
    verified_card_link = None
    card_short_id = _safe_str(payload.get("cardShortId")) or extract_short_id(_safe_str(payload.get("cardLink")))
    if card_id:
        loop = asyncio.get_running_loop()
        card_data = await loop.run_in_executor(None, fetch_card_details_sync, card_id)
        if card_data:
            card_details_str = format_card_details(card_data)
            canonical_card_id = normalize_canonical_trello_id(card_data.get("id")) or _safe_str(card_data.get("id")) or canonical_card_id
            if canonical_card_id:
                card_id = canonical_card_id
            verified_card_link = _safe_str(card_data.get("url")) or _safe_str(card_data.get("shortUrl"))
            live_short = _safe_str(card_data.get("shortLink")) or extract_short_id(verified_card_link)
            if live_short:
                trusted_short_id = live_short
                if not verified_card_link:
                    verified_card_link = f"https://trello.com/c/{live_short}"
                if is_canonical_trello_id(canonical_card_id):
                    record_verified_card_alias(live_short, canonical_card_id, card_url=verified_card_link)
                existing_link_short = extract_short_id(_safe_str(payload.get("cardLink")))
                existing_short = _safe_str(payload.get("cardShortId"))
                if (existing_link_short and existing_link_short != live_short) or (existing_short and existing_short != live_short):
                    logging.warning(
                        "[Trello Sidecar] Replacing mismatched card shortLink/link (%s/%s) with verified shortLink %s for card %s",
                        existing_short,
                        existing_link_short,
                        live_short,
                        card_id,
                    )
                card_short_id = live_short
            else:
                card_short_id = None
            _project_verified_card_identity(
                payload,
                canonical_id=canonical_card_id or card_id,
                verified_short_id=live_short,
                verified_url=verified_card_link,
            )
            card_name = _safe_str(card_data.get("name")) or card_name
            if isinstance(card_data.get("desc"), str):
                card_desc = card_data["desc"]
            if isinstance(card_data.get("list"), dict):
                list_name = _safe_str(card_data["list"].get("name")) or list_name
            if not comment and action_type == "commentCard" and action_id:
                matched_comment = resolve_comment_action(card_data, action_id=action_id)
                if matched_comment:
                    resolved_text = (matched_comment.get("data") or {}).get("text") or ""
                    if isinstance(resolved_text, str) and resolved_text:
                        comment = resolved_text
                        payload["comment"] = resolved_text
                        comment_from_payload = True
        else:
            canonical_card_id = normalize_canonical_trello_id(card_id) or normalize_canonical_trello_id(card_key)
            cached_info = lookup_verified_card_info(canonical_card_id or card_id or card_key)
            if cached_info:
                trusted_short_id = cached_info["short_link"]
                verified_card_link = cached_info["url"]
                card_short_id = trusted_short_id
                _project_verified_card_identity(
                    payload,
                    canonical_id=canonical_card_id or card_id,
                    verified_short_id=trusted_short_id,
                    verified_url=verified_card_link,
                )
            elif canonical_card_id or (card_key and card_key != card_name and card_key != "Unnamed Card") or card_id:
                card_short_id = None
                _project_verified_card_identity(
                    payload,
                    canonical_id=canonical_card_id or card_id,
                    verified_short_id=None,
                    verified_url=None,
                )

    if shutdown_event.is_set():
        raise ShutdownRequested("Server shutting down")

    trigger_username, trigger_name = extract_trigger_member(payload)
    if card_data and (comment_from_payload or (action_type == "commentCard" and action_id)) and (not trigger_username or not action_id):
        matched_action, unique_aid = resolve_comment_match(
            card_data,
            comment_text=comment,
            action_id=action_id,
            author_username=trigger_username,
        )
        if matched_action:
            member_creator = matched_action.get("memberCreator")
            if isinstance(member_creator, dict) and not trigger_username:
                resolved_username = (_safe_str(member_creator.get("username")) or "").lower()
                resolved_name = _safe_str(member_creator.get("fullName")) or resolved_username or "Unknown"
                if resolved_username:
                    payload["triggerUserUsername"] = resolved_username
                if resolved_name:
                    payload["triggerUserName"] = resolved_name
                trigger_username, trigger_name = resolved_username, resolved_name
            if not action_id and unique_aid:
                candidate_dkeys = _compute_candidate_delivery_keys(
                    card_key,
                    payload,
                    verified_short_ids=_dedup_strs([trusted_short_id]),
                )
                conflict_owned = False
                if candidate_dkeys:
                    with state_lock:
                        try:
                            chk_state = load_sidecar_state_strict()
                            conflict_owned = _inferred_action_owned_by_other_delivery(chk_state, unique_aid, candidate_dkeys)
                        except SidecarStateReadError:
                            conflict_owned = False
                if not conflict_owned:
                    action_id = unique_aid
                    action_id_inferred = True
                    payload["actionId"] = action_id

    quarantined = False
    with state_lock:
        try:
            q_state = load_sidecar_state_strict()
            q_in_flight = q_state.get("in_flight_cards")
            q_entry = q_in_flight.get(card_key) if isinstance(q_in_flight, dict) else None
            merged_entry = copy.deepcopy(q_entry) if isinstance(q_entry, dict) else {"payload": payload}
            merged_payload = merged_entry.setdefault("payload", {})
            if isinstance(merged_payload, dict) and isinstance(payload, dict):
                for k, v in payload.items():
                    if k not in ("_admission_id", "_defer_attempts", "_action_id_inferred"):
                        merged_payload[k] = v
            q_act, q_inf, q_dkeys, q_rfps, q_lfps, _ = _extract_row_quarantine_identities(
                q_state,
                card_key,
                merged_entry,
                action_id=action_id,
                action_id_inferred=action_id_inferred if action_id else None,
                extra_shorts=_dedup_strs([trusted_short_id]),
            )
            quarantined = is_quarantined_in_state(
                q_state,
                action_id=q_act,
                action_id_inferred=q_inf,
                delivery_keys=q_dkeys,
                replay_fingerprints=q_rfps,
                legacy_fingerprints=q_lfps,
            )
        except SidecarStateReadError:
            quarantined = False
    if quarantined:
        logging.warning(
            "[Trello Sidecar] Ignoring unreconciled trigger (action=%s) for card '%s' in worker recheck.",
            action_id or "id-less",
            card_name,
        )
        log_trigger_ignored(card_key, card_name, payload, comment, list_name, "unreconciled_action")
        return

    if is_agent_or_suppressed_comment(comment, trigger_username):
        logging.info(
            "[Trello Sidecar] Ignoring post-admission trigger from suppressed member/signature on card '%s' (member=%s)",
            card_name,
            trigger_username or trigger_name,
        )
        log_trigger_ignored(card_key, card_name, payload, comment, list_name, "suppressed_trigger_member")
        return
    if is_probable_agent_comment(comment, trigger_username, trigger_name):
        recent_comments = extract_recent_comment_texts(payload)
        if not recent_comments and card_data:
            recent_comments = [
                entry.get("text", "")
                for entry in extract_recent_comments_from_card_data(card_data, exclude_action_id=extract_action_id(payload))
                if entry.get("text")
            ]
        if is_low_signal_agent_comment(comment, recent_comments):
            logging.info(
                "[Trello Sidecar] Ignoring post-admission low-novelty agent comment on card '%s' (member=%s).",
                card_name,
                trigger_username or trigger_name,
            )
            log_trigger_ignored(card_key, card_name, payload, comment, list_name, "low_novelty_agent_comment")
            return

    mentioned_usernames = extract_mentioned_usernames(comment)
    trigger_user_info = ""
    if trigger_username:
        mentioned_info = ""
        if mentioned_usernames:
            mentioned_info = (
                f"- Usernames mentioned in the triggering comment text: "
                f"{', '.join('@' + username for username in mentioned_usernames)}\n"
            )
        trigger_user_info = (
            f"--- Webhook Triggering User ---\n"
            f"This action/discussion was triggered by the following Trello member:\n"
            f"- Name: {trigger_name}\n"
            f"- Trello Username: @{trigger_username}\n"
            f"{mentioned_info}"
            f"When replying on the card, address/tag the triggering comment author first. "
            f"Do not confuse usernames mentioned inside the comment with the author of the comment; mentioned usernames are only recipients/context. "
            f"Honor the Stakeholder Context and configured mention policies. Never @-mention the acting/posting account or any username whose mention policy is `never_at_mention`, because self-mentions and configured users can trigger automation loops.\n"
            f"---------------------------------\n\n"
        )
    elif mentioned_usernames:
        trigger_user_info = (
            f"--- Webhook Triggering User ---\n"
            f"The webhook payload did not identify the comment author. Usernames mentioned in the triggering comment text are: "
            f"{', '.join('@' + username for username in mentioned_usernames)}.\n"
            f"Do not assume a mentioned username wrote the comment. If live Trello card state does not identify the author, keep the response untagged or address the team generally.\n"
            f"---------------------------------\n\n"
        )
    
    # Resolve workspaces roles and git repo names
    workspaces_info = ""
    if WORKSPACES:
        workspaces_info += "### Loaded Workspaces:\n"
        for ws in WORKSPACES:
            role = "Unknown"
            if "frontend" in ws.lower() or "fe" in ws.lower():
                role = "Frontend (React/Next.js)"
            elif "backend" in ws.lower() or "be" in ws.lower():
                role = "Backend (Laravel/PHP)"
            
            git_repo = resolve_git_repo(ws)
            repo_info = f" (Git Repo: `{git_repo}`)" if git_repo else ""
            workspaces_info += f"- **{role}**: Path: `{ws}`{repo_info}\n"
    
    # Perform semantic classification
    phase = await classify_action(comment, card_name, card_desc, list_name, action_type=action_type)
    if phase == PHASE_BLOCKED_TOOL_ATTEMPT:
        log_sidecar_event("trigger_blocked", {
            "card_id": card_key,
            "card_name": card_name,
            "card_link": payload.get("cardLink"),
            "reason": "classifier_tool_disallowed",
            "list_name": list_name,
            "action_id": extract_action_id(payload),
        })
        return
    logging.info(f"[Trello Sidecar] Determined phase: {phase} for card '{card_name}'")
    log_sidecar_event("trigger_classified", {
        "card_id": card_key,
        "card_name": card_name,
        "card_link": payload.get("cardLink"),
        "phase": phase,
        "list_name": list_name,
        "action_id": extract_action_id(payload),
    })
    if phase == "READY_FOR_SPEC" and not verified_card_link:
        _defer_or_exhaust_queued_admission(
            card_key,
            card_name,
            payload,
            phase,
            list_name,
            action_id=action_id,
            admission_id=admission_id,
            reason="unverified_card_link",
            local_defer_attempts=local_defer_attempts,
        )
        return
    if shutdown_event.is_set():
        raise ShutdownRequested("Server shutting down")

    # Resolve conversation alias before ack or preflight so ambiguous canonical conflicts abort safely.
    card_link = verified_card_link
    session_read_failed = False
    ambiguous_canonical = False
    conversation_id = None
    conversation_aliases = []
    with state_lock:
        try:
            session_map = load_session_mapping_strict()
        except SessionMappingReadError:
            session_map = None
            session_read_failed = True
        if session_map is not None:
            before_aliases = dict(session_map)
            conversation_id, conversation_aliases, ambiguous_canonical = resolve_conversation_alias(
                session_map,
                card_key,
                card_name,
                card_link,
                card_short_id=card_short_id,
                canonical_id=canonical_card_id,
                trusted_short_id=trusted_short_id,
            )
            if conversation_id:
                logging.info(
                    "[Trello Sidecar] Found session %s for card '%s' via aliases: %s",
                    conversation_id,
                    card_name,
                    conversation_aliases,
                )
                if session_map != before_aliases:
                    save_session_mapping(session_map)
    if session_read_failed:
        _defer_or_exhaust_queued_admission(
            card_key,
            card_name,
            payload,
            phase,
            list_name,
            action_id=action_id,
            admission_id=admission_id,
            reason="session_map_read_failed",
            local_defer_attempts=local_defer_attempts,
        )
        return
    if ambiguous_canonical:
        logging.error(
            "[Trello Sidecar] Blocking trigger for card '%s': ambiguous conversation alias conflict requires resolution.",
            card_name,
        )
        log_sidecar_event("trigger_blocked", {
            "card_id": card_key,
            "card_name": card_name,
            "card_link": payload.get("cardLink"),
            "reason": "ambiguous_session_alias_conflict",
            "list_name": list_name,
            "action_id": extract_action_id(payload),
        })
        if not _release_undispatched_admission_with_dedup(
            card_key,
            admission_id=admission_id,
            action_id=action_id,
        ):
            _defer_or_exhaust_queued_admission(
                card_key,
                card_name,
                payload,
                phase,
                list_name,
                action_id=action_id,
                admission_id=admission_id,
                reason="ambiguous_session_alias_conflict_release_failed",
                local_defer_attempts=local_defer_attempts,
            )
        return

    if card_id and reserve_ack_comment(
        card_key,
        phase,
        action_id=extract_action_id(payload),
        admission_id=admission_id,
        action_id_inferred=action_id_inferred,
        short_card_id=trusted_short_id,
    ):
        if shutdown_event.is_set():
            raise ShutdownRequested("Server shutting down")
        loop = asyncio.get_running_loop()
        ack_sent = await loop.run_in_executor(None, post_trello_comment, card_id, build_ack_comment(), "acknowledgement")
        if shutdown_event.is_set():
            raise ShutdownRequested("Server shutting down")
        log_sidecar_event("ack_posted" if ack_sent else "ack_post_failed", {
            "card_id": card_key,
            "card_name": card_name,
            "card_link": payload.get("cardLink"),
            "phase": phase,
        })
    github_work_context = ""
    if phase == "READY_FOR_SPEC":
        loop = asyncio.get_running_loop()
        github_work_context = await loop.run_in_executor(
            None, collect_github_work_context, card_name, verified_card_link
        )
    stakeholder_context = build_stakeholder_context_block(trigger_username)
    codex_args_json = json.dumps(
        {
            "model": CODEX_REVIEW_MODEL,
            "config": {"model_reasoning_effort": "high"},
            "prompt": "[bounded review prompt]",
        }
    )

    # Choose the model and configure system instructions based on the phase
    model_name = DEFAULT_AGENT_MODEL
    if phase == "READY_FOR_SPEC":
        system_instruction = (
            "You are reviewing and responding to Trello cards as the PLANNER for the configured automation pipeline. "
            "Use the Trello API to interact with the specific card that triggered you (Trello credentials are available in the environment variables: "
            "TRELLO_API_KEY, TRELLO_SECRET, and TRELLO_API_TOKEN).\n\n"
            "This card is Ready for Spec (finalized/locked in). Your goal is to write a detailed specification and create or update GitHub issues without duplicating active work:\n"
            "1. First, perform a duplicate/progress check before writing specs or creating issues. Read the GitHub Duplicate/Progress Preflight context injected by the sidecar, then run your own targeted `gh issue list` and `gh pr list` searches in every relevant repository using the Trello card URL, short ID, title keywords, and any issue links already on the card.\n"
            "2. If a matching issue or PR already exists, do not create a duplicate. Instead, update/comment on the existing issue or PR with any missing Trello context, link it back to the card, and explain the current progress/status on Trello. Only create a new issue when the existing work is materially different, closed as intentionally not planned, or missing a required FE/BE counterpart.\n"
            "3. If duplicate status cannot be determined confidently, stop and post a Trello comment asking for human confirmation rather than creating a new issue.\n"
            "4. Create a detailed spec based on the title, description, and discussions in the card. Ensure it is grounded in the existing codebase.\n"
            "5. Once you write the draft plan, you must request an adversarial second opinion/review from Codex using the Codex MCP server:\n"
            f"   - {CODEX_REVIEW_PROMPT}\n"
            f"   - Call the `call_mcp_tool` tool with parameters exactly shaped as: `ServerName: \"codex-mcp\"`, `ToolName: \"codex\"`, and `Arguments: {codex_args_json}`. The `Arguments` object must contain a top-level `prompt` field.\n"
            "   - Refine and adjust your specification based on Codex's feedback/critique before proceeding.\n"
            "6. Create matching issues in the appropriate Frontend (FE) and Backend (BE) Github repositories only after the duplicate/progress check passes. The `gh` CLI is installed and pre-authenticated for this runtime environment. Use `gh issue create --repo <owner/repo> --title \"Title\" --body \"Body\"` instead of writing custom API scripts, and relate the issues to each other. **CRITICAL GUARDRAIL:** The body of every created GitHub issue MUST include a clear, direct link back to the originating Trello card (the Trello card link can be found in the webhook trigger payload as `cardLink` or in the card details as `url`, e.g., `https://trello.com/c/...`). This ensures context is not lost and allows status updates/syncing later.\n"
            "7. Link the created or reused GitHub issues/PRs back to the Trello card.\n"
            "8. Remove the 'Ready for Spec' label on the Trello card, add the 'Ready for Implementation' label, and move the card to the 'Ready for Implementation' list only after the GitHub work item set is confirmed non-duplicative.\n"
            "9. Relate specs to each other as appropriate, especially if there's both a FE and BE ticket as a result of the request.\n"
            "10. **Preserve QA/Discussion Context**: Since the QA, investigation, and design alignment conversations occurred asynchronously without engineering in the loop, you must summarize this context in your final specification. Detail what was asked during the grilling/QA phase, why it was asked, and what specific decision or option the PM/designer selected.\n"
            "11. **Comment Separation (CRITICAL)**: Do NOT post the detailed technical specification, file paths, class names, or database schemas as a Trello comment. Instead, write the detailed technical spec inside the GitHub issue bodies, and post only a short (under 120 words), product-focused, non-technical summary on the Trello card along with links to the created GitHub issues.\n"
            "12. Address your response/comments to the triggering comment author and relevant Trello stakeholders. Honor configured mention policies and never @-mention the acting/posting account or usernames marked `never_at_mention`. "
            f"Sign any card updates/comments with \"- Love {AGENT_SIGNATURE_NAME}\".\n\n"
            f"{TRELLO_HELPER_RULES}\n"
            "### Planning & Spec Rules\n\n"
            "Before writing any spec, plan or code, search the existing codebase first.\n\n"
            "#### Ground on the Main Branch\n"
            "Local working directories might be checked out to an in-progress, unstable, or outdated branch. Before analyzing code or writing a spec:\n"
            "- Always run `git fetch origin` first to ensure the local repository is aware of all upstream changes.\n"
            "- Check the git status and active branch. If it is not on the main/master branch, or if the branch is out of date, cross-reference and read the latest canonical files on the remote main branch (e.g. using `git show origin/main:path/to/file` or checking diffs via `git diff origin/main` to identify differences).\n"
            "- Base all specifications, file additions, and edits on the latest `origin/main` state rather than potentially stale or broken local feature branch code.\n\n"
            "#### Search Before You Build\n"
            "When a task requires a component, service, helper, hook, or utility — search the repo for an existing one before creating anything new.\n"
            "- **Frontend (Next.js/TS):** Need a modal? Search for existing modal components before creating `NewModal.tsx`. Need to fetch jobs? Check for existing hooks like `useJobs` or `usePagination` before writing a new one. Need a form input? Look for shared components in `/components` before building custom ones.\n"
            "- **Backend (Laravel/PHP):** Need to filter jobs? Check for existing query scopes on the model before writing raw query logic. Need to send a notification? Look for existing notification classes or a NotificationService before creating a new one. Need to format a response? Check for existing API Resources or transformers first.\n\n"
            "#### How to Write a Good Plan\n"
            "A good plan must include:\n"
            "1. **What already exists** — list the files, components, or services found that are relevant.\n"
            "2. **What will be reused** — explicitly state what is being reused and why.\n"
            "3. **What will be created new** — only if nothing suitable exists. Justify why.\n"
            "4. **API contract** — define the request/response shape if it touches both FE and BE.\n"
            "5. **Auth & permissions** — middleware, gates, or role checks needed.\n"
            "6. **DB migrations** — if the schema changes, list the migrations needed.\n"
            "7. **Error states** — plan for failure, what does the UI show? What does the API return?\n"
            "8. **Tests** — list what tests will be written. baselines are 80% coverage. Go beyond 80% for critical logic (payments, auth, scoring).\n"
            "9. **Acceptance criteria** — a numbered checklist the agent runs on itself.\n"
            "10. **Edge cases and assumptions** — call them out explicitly.\n"
            "11. **PM/QA Discussion Context** — summarize the questions asked during grilling, the reasons behind them, and the final design choices/approvals made by the PM.\n\n"
            "#### Acceptance Criteria Format\n"
            "Each criterion should be a concrete, checkable action.\n"
            "- [ ] Open the browser and navigate to `/jobs`.\n"
            "- [ ] Click the save button on a job card while logged in.\n"
            "- [ ] Verify the button state changes to \"Saved\".\n"
            "- [ ] Reload the page and confirm the saved state persists.\n"
            "- [ ] Open the network tab and confirm the correct API endpoint was called with the right payload.\n"
            "- [ ] Log out and confirm the button is not visible or is disabled for guests.\n"
            "- [ ] Confirm the tracking event fired.\n\n"
            "#### Default Behavior\n"
            "- Reuse over rebuild.\n"
            "- Extend over duplicate.\n"
            "- If you're unsure whether something exists, search.\n"
            "- Don't mark a task complete until every acceptance criterion is checked.\n"
            f"\n{AUDIENCE_RULES}\n"
            f"{GITHUB_ISSUE_QUALITY_RULES}\n"
            f"{PROCESS_AUTOMATION_RULES}\n"
            f"{SUPERPOWERS_RULES}\n"
        )
    elif phase == "INVESTIGATOR":
        system_instruction = (
            "You are reviewing and responding to Trello cards as the INVESTIGATOR for the configured automation pipeline. "
            "Use the Trello API to interact with the specific card that triggered you (Trello credentials are available in the environment variables: "
            "TRELLO_API_KEY, TRELLO_SECRET, and TRELLO_API_TOKEN).\n\n"
            "Use the grill-me skill to align on requirements and resolve design decisions through a structured, interactive interview tailored for Trello's asynchronous nature:\n"
            "1. **Asynchronous Batch Questioning**: Trello is asynchronous, not real-time. Ask only the questions needed to unblock product/design decisions in this turn. Usually ask 2-5 questions, not a long interrogation. Keep language simple, non-technical, and direct.\n"
            "2. **Manage PM/User Expectations**: Briefly state that follow-up questions may happen, but do not lecture the PM/designer about process.\n"
            "3. **Non-Technical POV (UI/UX First)**: Frame all discussions, questions, and option proposals from the perspective of user experience (UI/UX), visual layout, and product behavior rather than backend or database-level engineering. Use simple, friendly, and non-technical language tailored for PMs, designers, and other non-engineering stakeholders. You may include minor technical limits only if directly relevant (e.g. 'we currently limit users to a maximum of 3 resumes').\n"
            "4. **Identify UI/UX Chain Reactions**: Think holistically about the entire product journey. When a PM requests a feature or modification on a specific screen (e.g. the `/apply-with-ai` page), analyze how this change ripples across other parts of the system (e.g. the user's dashboard, settings, activity history, or billing). Explicitly point out these downstream UI/UX implications to the PMs so they can approve the full scope of the change.\n"
            "5. **Strict Gatekeeping (Do Not Skip to Spec)**: You MUST NOT transition to spec or implementation mode (and must not recommend moving the card to 'Ready for Spec') if there are still critical, unanswered questions—even if a PM or user explicitly tells you to go straight to spec or implementation. You must insist on getting answers or, at a minimum, an explicit acknowledgment from them that they have chosen to skip/bypass specific questions before you proceed.\n"
            "6. **Light Grounding Only Until Requirements Are Settled**: Do not do a deep code dive while critical product/design questions are still unanswered. Use only quick repository searches or prior context to avoid obviously impossible suggestions and to identify likely reusable surfaces. Save detailed file-by-file investigation, implementation planning, and Codex review for PLANNER mode after decisions are settled.\n"
            "7. **Formulate Options**: If enough is known to discuss direction, propose up to three clear approaches:\n"
            "   - A quick/easy version (reusing existing components/logic to the maximum).\n"
            "   - An ideal version (perfectly engineered design).\n"
            "   - A compromise version (reasonable trade-off between speed and clean architecture).\n"
            "   - Keep the options product/design-facing. Do not name files/classes unless explicitly asked.\n"
            "8. **Optional Codex Review**: Use Codex only when requirements are mostly settled and the options materially affect implementation architecture, cross-screen product behavior, permissions, billing, data integrity, or automation risk. Skip Codex for unanswered requirements, simple copy/layout questions, status replies, or one-screen clarifications. When you do use Codex, use the bounded review format from the planning rules and keep the Trello-facing result non-technical.\n"
            f"9. **Post Comments & Tag**: Post your final refined response as a comment on the Trello card. You MUST address the user who triggered/commented on the card and relevant stakeholders. Honor configured mention policies and never @-mention the acting/posting account or usernames marked `never_at_mention`. Keep sentences short and use bullet points for readability. Sign with \"- Love {AGENT_SIGNATURE_NAME}\".\n\n"
            f"{AUDIENCE_RULES}\n"
            f"{PROCESS_AUTOMATION_RULES}\n"
            f"{SUPERPOWERS_RULES}\n"
            f"{TRELLO_HELPER_RULES}"
        )
    else:
        system_instruction = (
            "You are reviewing and responding to Trello cards as the GENERAL responder for the configured automation pipeline. "
            "Use the Trello API to interact with the specific card that triggered you (Trello credentials are available in the environment variables: "
            "TRELLO_API_KEY, TRELLO_SECRET, and TRELLO_API_TOKEN).\n\n"
            "This is a General Discussion trigger. The comment is conversational, seeking general feedback/ideas or asking general questions:\n"
            "1. Respond constructively and collaboratively as appropriate.\n"
            "2. Keep language simple, non-technical, and scannable. Answer simple questions directly before adding context.\n"
            "3. **Non-Technical POV (UI/UX First)**: Frame your answers around user experience and interface presentation. Speak in user-facing terms rather than backend system behaviors, keeping the non-engineering audience (PMs, designers) in mind.\n"
            f"4. Post your response as a comment on the Trello card, addressing the triggering comment author and relevant stakeholders. Honor configured mention policies and never @-mention the acting/posting account or usernames marked `never_at_mention`. Sign with \"- Love {AGENT_SIGNATURE_NAME}\".\n\n"
            f"{AUDIENCE_RULES}\n"
            f"{PROCESS_AUTOMATION_RULES}\n"
            f"{SUPERPOWERS_RULES}\n"
            f"{TRELLO_HELPER_RULES}"
        )

    previous_conversation_context = collect_previous_conversation_context(conversation_id)
    target_cwd = valid_cwd(resolve_target_cwd(card_name, card_desc, list_name, WORKSPACES))

    if conversation_id:
        logging.info(f"[Trello Sidecar] Resuming existing conversation {conversation_id} for card '{card_name}' using model '{model_name}'")
    else:
        logging.info(f"[Trello Sidecar] Starting new conversation thread for card '{card_name}' using model '{model_name}'")

    prompt_payload = project_prompt_payload(
        payload,
        canonical_id=card_id or card_key,
        verified_short_id=card_short_id,
        verified_url=verified_card_link,
        card_name=card_name,
        list_name=list_name,
        comment=comment,
        action_type=action_type,
        action_id=action_id,
        trigger_username=trigger_username,
        trigger_name=trigger_name,
    )
    payload_str = json.dumps(prompt_payload, indent=2)
    trigger_summary = build_compact_trigger_summary(
        prompt_payload,
        card_id=card_id or card_key,
        card_name=card_name,
        card_link=verified_card_link,
        card_short_id=card_short_id,
        list_name=list_name,
        comment=comment,
        trigger_username=trigger_username,
        trigger_name=trigger_name,
    )

    security_rules = (
        "CRITICAL SECURITY REQUIREMENT:\n"
        "- Do NOT print, comment, or reveal any credentials, API keys, secrets, tokens, or passwords under any circumstances. "
        "This applies to Trello tokens/keys, database passwords (like db_reader), .env file contents, or any frontend/backend configuration keys. "
        "If referring to configuration state, only indicate presence (e.g. 'verified TRELLO_API_KEY is configured') and NEVER print/disclose the actual values.\n"
        "- STRICT READ-ONLY GUARDRAIL: You are strictly in an investigation and planning phase. You are STRICTLY FORBIDDEN from editing, creating, or deleting any files in the workspace repositories. "
        "Do NOT use tools like write_file, edit_file, write_to_file, replace_file_content, multi_replace_file_content, sed_file, or run commands that modify code. Base your specifications and options purely on reading the code."
    )

    prompt = build_agent_prompt(
        system_instruction=system_instruction,
        security_rules=security_rules,
        payload_str=payload_str,
        trigger_user_info=trigger_user_info,
        stakeholder_context=stakeholder_context,
        workspaces_info=workspaces_info,
        github_work_context=github_work_context,
        previous_conversation_context=previous_conversation_context,
        card_details_str=card_details_str,
        trigger_summary=trigger_summary,
        max_bytes=MAX_PROMPT_BYTES,
    )

    if shutdown_event.is_set():
        raise ShutdownRequested("Server shutting down")

    started_at = time.time()
    linked_conversation_id = conversation_id
    attempt_conversation_id = conversation_id
    last_error = None
    dispatched_recorded = False

    def handle_conversation_id(new_id):
        nonlocal conversation_id, linked_conversation_id
        cid = validate_conversation_id(new_id)
        if not cid or cid == linked_conversation_id:
            return
        if persist_conversation_id(conversation_aliases, cid, expected_existing=linked_conversation_id):
            linked_conversation_id = cid
            conversation_id = cid
            logging.info("[Trello Sidecar] Linked conversation ID %s for card '%s'", cid, card_name)
            log_sidecar_event("conversation_linked", {
                "card_id": card_key,
                "card_name": card_name,
                "card_link": payload.get("cardLink"),
                "conversation_id": cid,
            })
        else:
            logging.error("[Trello Sidecar] Failed to persist conversation ID %s for card '%s'", cid, card_name)
            log_sidecar_event("conversation_link_failed", {
                "card_id": card_key,
                "card_name": card_name,
                "card_link": payload.get("cardLink"),
                "conversation_id": cid,
            })

    try:
        for attempt in range(2):
            if attempt:
                logging.warning(
                    "[Trello Sidecar] Retrying agent run once for card '%s' after %s",
                    card_name,
                    type(last_error).__name__,
                )
                log_sidecar_event("agent_run_retry", {
                    "card_id": card_key,
                    "card_name": card_name,
                    "card_link": payload.get("cardLink"),
                    "model": model_name,
                    "conversation_id": attempt_conversation_id,
                    "attempt": attempt + 1,
                })
                started_at = time.time()

            attempt_cmd = build_agy_command(
                model_name,
                prompt,
                workspaces=WORKSPACES,
                target_cwd=target_cwd,
                conversation_id=attempt_conversation_id,
                print_timeout=AGENT_PRINT_TIMEOUT,
                output_format="stream-json",
            )
            if attempt == 0 or attempt_conversation_id != conversation_id:
                logging.info(f"[Trello Sidecar] Running agy command: {format_command_for_log(attempt_cmd)}")

            process = None
            launched = False
            launch_started = False
            try:
                if shutdown_event.is_set():
                    raise ShutdownRequested("Server shutting down")
                newly_dispatched = False
                if not dispatched_recorded:
                    dispatch_detail = {}
                    if not mark_in_flight_dispatched(
                        card_key,
                        conversation_id=conversation_id,
                        admission_id=admission_id,
                        action_id=action_id,
                        action_id_inferred=action_id_inferred,
                        short_card_id=trusted_short_id,
                        detail=dispatch_detail,
                    ):
                        dispatch_status = dispatch_detail.get("status")
                        if dispatch_status == "quarantined":
                            log_trigger_ignored(card_key, card_name, payload, comment, list_name, "unreconciled_action")
                            return
                        if dispatch_status == "identity_conflict":
                            log_trigger_ignored(card_key, card_name, payload, comment, list_name, "identity_conflict")
                            return
                        if dispatch_status == "transient_error":
                            _defer_or_exhaust_queued_admission(
                                card_key,
                                card_name,
                                payload,
                                phase,
                                list_name,
                                action_id=action_id,
                                admission_id=admission_id,
                                reason="dispatch_state_persistence_failed",
                                local_defer_attempts=local_defer_attempts,
                            )
                            return
                        log_sidecar_event("agent_run_error", {
                            "card_id": card_key,
                            "card_name": card_name,
                            "card_link": payload.get("cardLink"),
                            "model": model_name,
                            "conversation_id": conversation_id,
                            "error_code": "state_persistence_failed",
                        })
                        raise _DispatchStateError("Failed to persist dispatched trigger state")
                    dispatched_recorded = True
                    newly_dispatched = True
                aborted_before_spawn = False
                spawn_still_open = True
                with _spawn_gate_lock:
                    if not begin_process_launch():
                        aborted_before_spawn = True
                    else:
                        launch_started = True
                        try:
                            process = await asyncio.create_subprocess_exec(
                                *attempt_cmd,
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE,
                                cwd=target_cwd,
                                start_new_session=True,
                                limit=1024 * 1024,
                            )
                            launched = True
                        finally:
                            spawn_still_open = finish_process_launch(process)
                            launch_started = False
                if aborted_before_spawn:
                    if newly_dispatched:
                        rollback_dispatched_to_queued(card_key, admission_id=admission_id)
                    raise ShutdownRequested("Server shutting down")
                if not spawn_still_open or shutdown_event.is_set():
                    await terminate_process(process)
                    raise ShutdownRequested("Server shutting down")
                if newly_dispatched:
                    log_sidecar_event("agent_run_started", {
                        "card_id": card_key,
                        "card_name": card_name,
                        "card_link": payload.get("cardLink"),
                        "model": model_name,
                        "conversation_id": conversation_id,
                        "workspace": target_cwd,
                    })

                stream_res = await consume_agy_stream(
                    process,
                    AGENT_SUBPROCESS_TIMEOUT_SECONDS,
                    on_conversation_id=handle_conversation_id,
                    allow_tools=True,
                )

                if shutdown_event.is_set():
                    raise ShutdownRequested("Server shutting down")

                parsed = stream_res["result"]
                if stream_res["timed_out"]:
                    error_code = "timeout"
                    logging.error(
                        "[Trello Sidecar] Agent process timed out after %ss for card '%s' (saw_tool_call=%s, framing_lost=%s)",
                        AGENT_SUBPROCESS_TIMEOUT_SECONDS,
                        card_name,
                        stream_res["saw_tool_call"],
                        stream_res["framing_lost"],
                    )
                    log_sidecar_event("agent_run_failed", {
                        "card_id": card_key,
                        "card_name": card_name,
                        "card_link": payload.get("cardLink"),
                        "model": model_name,
                        "conversation_id": linked_conversation_id or attempt_conversation_id,
                        "duration_ms": int((time.time() - started_at) * 1000),
                        "returncode": stream_res["returncode"],
                        "attempt": attempt + 1,
                        "error_code": error_code,
                        "saw_tool_call": stream_res["saw_tool_call"],
                        "framing_lost": stream_res["framing_lost"],
                    })
                    last_error = TimeoutError(f"Agent timed out after {AGENT_SUBPROCESS_TIMEOUT_SECONDS}s")
                    if not is_retryable_pre_tool_failure(stream_res, parsed, error_code=error_code):
                        break
                    continue

                ok, err_reason = evaluate_agy_result(
                    stream_res["returncode"],
                    parsed,
                    stream_res["raw_stderr"],
                    require_conversation_id=True,
                    init_conversation_id=stream_res["init_conversation_id"],
                )

                if ok:
                    final_id = validate_conversation_id((parsed or {}).get("conversation_id")) or validate_conversation_id(stream_res["init_conversation_id"])
                    handle_conversation_id(final_id)
                    response_text = str((parsed or {}).get("response") or "")
                    logging.info(
                        "[Trello Sidecar] Agent task completed successfully for card '%s' (conversation_id=%s, response_len=%d)",
                        card_name,
                        conversation_id,
                        len(response_text),
                    )
                    log_sidecar_event("agent_run_completed", {
                        "card_id": card_key,
                        "card_name": card_name,
                        "card_link": payload.get("cardLink"),
                        "model": model_name,
                        "conversation_id": conversation_id,
                        "duration_ms": int((time.time() - started_at) * 1000),
                        "returncode": stream_res["returncode"],
                    })
                    return

                error_code = classify_error_code(
                    stream_res["returncode"],
                    parsed,
                    stream_res["raw_stderr"],
                    timed_out=False,
                    tool_disallowed=stream_res["tool_disallowed"],
                    framing_lost=stream_res["framing_lost"],
                )
                logging.error(
                    "[Trello Sidecar] Agent process failed (rc=%s, error_code=%s, saw_tool_call=%s, framing_lost=%s) for card '%s'",
                    stream_res["returncode"],
                    error_code,
                    stream_res["saw_tool_call"],
                    stream_res["framing_lost"],
                    card_name,
                )
                log_sidecar_event("agent_run_failed", {
                    "card_id": card_key,
                    "card_name": card_name,
                    "card_link": payload.get("cardLink"),
                    "model": model_name,
                    "conversation_id": linked_conversation_id or attempt_conversation_id,
                    "duration_ms": int((time.time() - started_at) * 1000),
                    "returncode": stream_res["returncode"],
                    "attempt": attempt + 1,
                    "error_code": error_code,
                    "saw_tool_call": stream_res["saw_tool_call"],
                    "framing_lost": stream_res["framing_lost"],
                })
                last_error = RuntimeError(f"Agent failed ({error_code})")
                if not is_retryable_pre_tool_failure(stream_res, parsed, error_code=error_code):
                    break
                if attempt == 0 and attempt_conversation_id and should_drop_conversation_on_retry(err_reason, stream_res["raw_stderr"]):
                    logging.warning(
                        "[Trello Sidecar] Dropping failed conversation %s for retry on card '%s'.",
                        attempt_conversation_id,
                        card_name,
                    )
                    attempt_conversation_id = None
            except (ShutdownRequested, _DispatchStateError, DeferredTriggerRetry):
                raise
            except Exception as e:
                logging.error(
                    "[Trello Sidecar] Error executing agy CLI subprocess: %s",
                    type(e).__name__,
                )
                log_sidecar_event("agent_run_error", {
                    "card_id": card_key,
                    "card_name": card_name,
                    "card_link": payload.get("cardLink"),
                    "model": model_name,
                    "conversation_id": linked_conversation_id or attempt_conversation_id,
                    "duration_ms": int((time.time() - started_at) * 1000),
                    "error_code": "post_launch_error" if launched else "subprocess_launch_error",
                })
                last_error = e
                if launched:
                    break
            finally:
                if launch_started:
                    finish_process_launch(process)
                if process:
                    await terminate_process(process)
                    unregister_active_process(process)
    except _DispatchStateError as exc:
        raise RuntimeError(str(exc)) from exc

    if shutdown_event.is_set():
        raise ShutdownRequested("Server shutting down")

    # Both attempts failed: surface the failure on the Trello card, then propagate.
    logging.error(f"[Trello Sidecar] Agent run failed for card '{card_name}'; posting failure comment.")
    if card_id:
        loop = asyncio.get_running_loop()
        failure_sent = await loop.run_in_executor(None, post_trello_comment, card_id, build_failure_comment(), "failure")
        if shutdown_event.is_set():
            raise ShutdownRequested("Server shutting down")
        log_sidecar_event("failure_comment_posted" if failure_sent else "failure_comment_failed", {
            "card_id": card_key,
            "card_name": card_name,
            "card_link": payload.get("cardLink"),
        })
    raise last_error

# Create a thread-safe queue for Trello agent triggers
task_queue = queue.Queue()

# ThreadPoolExecutor to run up to 3 agents concurrently
executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
queue_worker_started = False

def _requeue_deferred_task(card_key, card_name_str, payload):
    if shutdown_event.is_set():
        with state_lock:
            pending_cards.discard(card_key)
            norm_key = normalize_canonical_trello_id(card_key)
            if norm_key:
                pending_cards.discard(norm_key)
        return
    task_queue.put((card_key, card_name_str, payload))

def run_agent_job(card_key, card_name_str, payload):
    logging.info(f"[Trello Sidecar] Worker thread starting execution for card: '{card_name_str}'")
    shutting_down = False
    deferred_retry = False
    admission_id = payload.get("_admission_id") if isinstance(payload, dict) else None
    try:
        asyncio.run(trigger_agent(card_key, card_name_str, payload))
    except DeferredTriggerRetry as exc:
        deferred_retry = True
        if exc.defer_attempts is not None and isinstance(payload, dict):
            payload["_defer_attempts"] = exc.defer_attempts
        if not shutdown_event.is_set():
            timer = threading.Timer(
                exc.delay_seconds,
                _requeue_deferred_task,
                args=(card_key, card_name_str, payload),
            )
            timer.daemon = True
            timer.start()
    except ShutdownRequested:
        shutting_down = True
        logging.info("[Trello Sidecar] Aborted card '%s' due to server shutdown.", card_name_str)
    except Exception as e:
        logging.error(
            "[Trello Sidecar] Concurrent agent execution ended with %s for card '%s'",
            type(e).__name__,
            card_name_str,
        )
    finally:
        if deferred_retry and not shutdown_event.is_set():
            pass
        elif shutting_down or shutdown_event.is_set():
            with state_lock:
                pending_cards.discard(card_key)
                norm_key = normalize_canonical_trello_id(card_key)
                if norm_key:
                    pending_cards.discard(norm_key)
        else:
            release_pending_card(card_key, admission_id=admission_id)

def queue_worker():
    logging.info("[Trello Sidecar] Background queue worker thread started successfully.")
    while not shutdown_event.is_set():
        try:
            card_key, card_name_str, payload = task_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            if shutdown_event.is_set():
                break
            logging.info(f"[Trello Sidecar] Worker pulled task for card: '{card_name_str}'. Submitting to executor...")
            executor.submit(run_agent_job, card_key, card_name_str, payload)
        except Exception as e:
            logging.error(f"[Trello Sidecar] Worker exception submitting task: {e}")
        finally:
            task_queue.task_done()

def start_queue_worker():
    global queue_worker_started
    if queue_worker_started:
        return
    threading.Thread(target=queue_worker, daemon=True).start()
    queue_worker_started = True

def _unreconciled_key(unreconciled, norm_key, action_id, admission_id, now, tombstones=None):
    suffix = _safe_str(admission_id) or f"run-{secrets.token_hex(8)}"
    base = f"{norm_key}:{suffix}"
    candidate = base
    idx = 1
    while candidate in unreconciled or (isinstance(tombstones, dict) and candidate in tombstones):
        idx += 1
        candidate = f"{base}#{idx}"
    return candidate

def _entry_fingerprint(norm_key, entry):
    payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
    return entry.get("fingerprint") or trigger_fingerprint(
        norm_key,
        payload,
        payload.get("comment") or "",
        payload.get("listName") or "",
    )

def _record_quarantine_entry(
    state,
    unreconciled,
    norm_key,
    card_name_str,
    payload,
    action_id,
    admission_id,
    fingerprint,
    conv_id,
    stage,
    dispatched_at,
    now,
    save_fp=False,
    replay_fingerprints=None,
    short_card_id=None,
    action_id_inferred=False,
    delivery_keys=None,
    legacy_headerless=False,
):
    replay_fps = _dedup_strs(replay_fingerprints)
    legacy_fps = [fingerprint] if (fingerprint and not replay_fps and (not action_id or save_fp)) else []
    dkeys = _dedup_strs(delivery_keys)
    record = {
        "card_id": norm_key,
        "card_name": card_name_str,
        "card_link": payload.get("cardLink"),
        "action_id": action_id,
        "action_id_inferred": bool(action_id and action_id_inferred),
        "delivery_keys": dkeys,
        "admission_id": admission_id,
        "fingerprint": fingerprint,
        "replay_fingerprints": replay_fps if (not action_id or save_fp) else [],
        "short_card_id": short_card_id,
        "admitted_without_action_id": bool(save_fp or not action_id),
        "conversation_id": conv_id,
        "stage": stage,
        "dispatched_at": dispatched_at,
        "interrupted_at": now,
    }
    if legacy_headerless:
        record["legacy_headerless"] = True
    tombstones = state.setdefault("unreconciled_tombstones", {})
    u_key = _unreconciled_key(unreconciled, norm_key, action_id, admission_id, now, tombstones=tombstones)
    unreconciled[u_key] = record
    if isinstance(tombstones, dict):
        tomb = {
            "record_id": u_key,
            "card_id": norm_key,
            "short_card_id": short_card_id,
            "action_id": action_id,
            "action_id_inferred": bool(action_id and action_id_inferred),
            "delivery_keys": dkeys,
            "fingerprints": replay_fps if (not action_id or save_fp) else [],
            "legacy_fingerprints": legacy_fps,
            "interrupted_at": now,
        }
        if admission_id:
            tomb["admission_id"] = admission_id
        if legacy_headerless:
            tomb["legacy_headerless"] = True
        if fingerprint:
            tomb["cooldown_fingerprint"] = fingerprint
        tombstones[u_key] = tomb
    return record

def _is_duplicate_queued_row(winner_entry, winner_ident, other_entry, other_ident):
    w_adm = _safe_str(winner_entry.get("admission_id")) if isinstance(winner_entry, dict) else None
    o_adm = _safe_str(other_entry.get("admission_id")) if isinstance(other_entry, dict) else None
    if w_adm and o_adm and w_adm == o_adm:
        return True
    w_act, w_inf, w_dkeys, w_rfps, _, _ = winner_ident
    o_act, o_inf, o_dkeys, o_rfps, _, _ = other_ident
    w_dset = set(w_dkeys or ())
    o_dset = set(o_dkeys or ())
    if w_dset and o_dset:
        if w_dset & o_dset:
            return True
        return bool(w_act and o_act and w_act == o_act and not w_inf and not o_inf)
    if w_act and o_act:
        if w_act == o_act:
            if not w_inf and not o_inf:
                return True
            return bool(w_inf and o_inf and (set(w_rfps or ()) & set(o_rfps or ())))
        return False
    if not w_dset and not o_dset and (set(w_rfps or ()) & set(o_rfps or ())):
        return True
    return False

def _sync_entry_action_identity(entry, action_id, action_id_inferred):
    clean_act = _safe_str(action_id)
    if not isinstance(entry, dict) or not clean_act:
        return False
    changed = False
    clean_inf = bool(action_id_inferred)
    if _safe_str(entry.get("action_id")) != clean_act:
        entry["action_id"] = clean_act
        entry["action_id_inferred"] = clean_inf
        changed = True
    elif bool(entry.get("action_id_inferred")) != clean_inf or (
        "action_id_inferred" in entry and entry.get("action_id_inferred") is not clean_inf
    ):
        entry["action_id_inferred"] = clean_inf
        changed = True
    payload = entry.get("payload")
    if not isinstance(payload, dict):
        payload = {}
        entry["payload"] = payload
        changed = True
    action_obj = payload.get("action")
    if isinstance(action_obj, dict) and _safe_str(action_obj.get("id")) and _safe_str(action_obj.get("id")) != clean_act:
        action_obj["id"] = clean_act
        changed = True
    if _safe_str(payload.get("actionId")) and _safe_str(payload.get("actionId")) != clean_act:
        payload["actionId"] = clean_act
        changed = True
    if _safe_str(payload.get("idAction")) and _safe_str(payload.get("idAction")) != clean_act:
        payload["idAction"] = clean_act
        changed = True
    if extract_action_id(payload) != clean_act:
        payload["actionId"] = clean_act
        changed = True
    return changed

def requeue_in_flight_cards():
    """Re-enqueue safely queued cards and quarantine already-dispatched interrupted runs."""
    queued_items = []
    interrupted_items = []
    quarantined_queued_logs = []
    with state_lock:
        try:
            state = load_sidecar_state_strict()
        except SidecarStateReadError:
            logging.error("[Trello Sidecar] Failed to read sidecar state on startup; skipping in-flight recovery.")
            log_sidecar_event("in_flight_reconcile_failed", {"reason": "state_read_error"})
            return False
        now = time.time()
        state_dirty = _ensure_unified_tombstones(state, now=now)
        unreconciled = state.setdefault("unreconciled_cards", {})
        if not isinstance(unreconciled, dict):
            unreconciled = {}
            state["unreconciled_cards"] = unreconciled
        in_flight = state.get("in_flight_cards")
        if not isinstance(in_flight, dict) or not in_flight:
            if len(unreconciled) > 500:
                for old_key in list(unreconciled.keys())[:-500]:
                    unreconciled.pop(old_key, None)
                state_dirty = True
            if state_dirty and not save_sidecar_state(state):
                logging.error("[Trello Sidecar] Failed to persist tombstone migration on startup.")
                log_sidecar_event("in_flight_reconcile_failed", {"reason": "tombstone_migration_save_failed"})
                return False
            return True
        session_map = None
        aliases_map = state.get("card_aliases") if isinstance(state.get("card_aliases"), dict) else {}
        grouped = {}
        for raw_key, entry in list(in_flight.items()):
            raw_str = str(raw_key or "").strip()
            norm_key = normalize_canonical_trello_id(raw_str)
            if not norm_key:
                bridged = normalize_canonical_trello_id(aliases_map.get(raw_str))
                norm_key = bridged or raw_str
            grouped.setdefault(norm_key, []).append((raw_key, entry if isinstance(entry, dict) else {}))

        grouped_dispatched = {}
        grouped_queued = {}
        for norm_key, rows in grouped.items():
            for raw_key, entry in rows:
                stage = entry.get("stage")
                if stage == "queued":
                    grouped_queued.setdefault(norm_key, []).append((raw_key, entry))
                else:
                    grouped_dispatched.setdefault(norm_key, []).append((raw_key, entry))
                if raw_key != norm_key or stage != "queued" or len(rows) > 1:
                    if in_flight.pop(raw_key, None) is not None and raw_key != norm_key:
                        state_dirty = True

        # Pass 1: Quarantine all dispatched/unknown interrupted runs into unreconciled_tombstones.
        for norm_key, dispatched_rows in grouped_dispatched.items():
            for raw_key, entry in dispatched_rows:
                state_dirty = True
                payload = entry.get("payload") or {}
                card_name_str = _safe_str(entry.get("card_name")) or _safe_str(payload.get("cardName")) or "Unnamed Card"
                action_id, action_inferred, delivery_keys, replay_fps, _, short_card_id = _extract_row_quarantine_identities(
                    state,
                    norm_key,
                    entry,
                    raw_key=raw_key,
                )
                fingerprint = _entry_fingerprint(norm_key, entry)
                conv_id = validate_conversation_id(entry.get("conversation_id"))
                if not conv_id:
                    if session_map is None:
                        session_map = load_session_mapping()
                    conv_id = validate_conversation_id(session_map.get(norm_key) or session_map.get(str(raw_key)))
                record = _record_quarantine_entry(
                    state,
                    unreconciled,
                    norm_key,
                    card_name_str,
                    payload,
                    action_id,
                    entry.get("admission_id"),
                    fingerprint,
                    conv_id,
                    entry.get("stage") or "unknown",
                    entry.get("dispatched_at"),
                    now,
                    save_fp=bool(entry.get("admitted_without_action_id") or not action_id),
                    replay_fingerprints=replay_fps,
                    short_card_id=short_card_id,
                    action_id_inferred=action_inferred,
                    delivery_keys=delivery_keys,
                    legacy_headerless=_is_legacy_headerless_entry(entry, entry_delivery_keys=delivery_keys),
                )
                interrupted_items.append((norm_key, card_name_str, payload, record))

        # Pass 2: Screen every queued row against the full quarantine (existing + Pass 1).
        for norm_key, queued_rows in grouped_queued.items():
            eligible_queued = []
            for raw_key, entry in queued_rows:
                payload = entry.get("payload") or {}
                card_name_str = _safe_str(entry.get("card_name")) or _safe_str(payload.get("cardName")) or "Unnamed Card"
                q_act, q_inf, q_dkeys, q_rfps, q_lfps, _ = _extract_row_quarantine_identities(
                    state,
                    norm_key,
                    entry,
                    raw_key=raw_key,
                )
                if is_quarantined_in_state(
                    state,
                    action_id=q_act,
                    action_id_inferred=q_inf,
                    delivery_keys=q_dkeys,
                    replay_fingerprints=q_rfps,
                    legacy_fingerprints=q_lfps,
                ):
                    in_flight.pop(raw_key, None)
                    state_dirty = True
                    comment_val = payload.get("comment") if isinstance(payload.get("comment"), str) else ""
                    list_val = _safe_str(payload.get("listName")) or ""
                    quarantined_queued_logs.append((norm_key, card_name_str, payload, comment_val, list_val))
                else:
                    eligible_queued.append((raw_key, entry))

            if eligible_queued:
                eligible_queued.sort(
                    key=lambda item: (
                        1 if item[0] == norm_key else 0,
                        float(item[1].get("queued_at") or 0),
                        str(item[0]),
                    ),
                    reverse=True,
                )
                winner_key, winner_entry = eligible_queued[0]
                winner_ident = _extract_row_quarantine_identities(
                    state,
                    norm_key,
                    winner_entry,
                    raw_key=winner_key,
                )
                retained_entry = copy.deepcopy(winner_entry)
                for conflict_key, conflict_entry in eligible_queued[1:]:
                    state_dirty = True
                    conflict_ident = _extract_row_quarantine_identities(
                        state,
                        norm_key,
                        conflict_entry,
                        raw_key=conflict_key,
                    )
                    if _is_duplicate_queued_row(winner_entry, winner_ident, conflict_entry, conflict_ident):
                        if conflict_entry.get("ack_reserved"):
                            retained_entry["ack_reserved"] = True
                        if conflict_entry.get("short_card_id") and not retained_entry.get("short_card_id"):
                            retained_entry["short_card_id"] = conflict_entry["short_card_id"]
                        if conflict_entry.get("delivery_keys") or retained_entry.get("delivery_keys"):
                            retained_entry["delivery_keys"] = _dedup_strs([
                                *(retained_entry.get("delivery_keys") or ()),
                                *(conflict_entry.get("delivery_keys") or ()),
                            ])
                        if conflict_entry.get("replay_fingerprints") or retained_entry.get("replay_fingerprints"):
                            retained_entry["replay_fingerprints"] = _dedup_strs([
                                *(retained_entry.get("replay_fingerprints") or ()),
                                *(conflict_entry.get("replay_fingerprints") or ()),
                            ])
                        if conflict_entry.get("admitted_without_action_id"):
                            retained_entry["admitted_without_action_id"] = True
                        r_act = _safe_str(retained_entry.get("action_id")) or extract_action_id(retained_entry.get("payload"))
                        r_inf = bool(retained_entry.get("action_id_inferred")) if r_act else False
                        c_act = _safe_str(conflict_entry.get("action_id")) or extract_action_id(conflict_entry.get("payload"))
                        c_inf = bool(conflict_entry.get("action_id_inferred")) if c_act else False
                        if c_act and (not r_act or (r_inf and not c_inf)):
                            _sync_entry_action_identity(retained_entry, c_act, c_inf)
                        if isinstance(conflict_entry.get("defer_attempts"), int):
                            retained_entry["defer_attempts"] = max(
                                int(retained_entry.get("defer_attempts") or 0),
                                conflict_entry["defer_attempts"],
                            )
                        continue
                    c_payload = conflict_entry.get("payload") or {}
                    c_name = _safe_str(conflict_entry.get("card_name")) or _safe_str(c_payload.get("cardName")) or "Unnamed Card"
                    c_fp = _entry_fingerprint(norm_key, conflict_entry)
                    c_act, c_inf, c_dkeys, c_replay_fps, _, c_short_id = conflict_ident
                    c_record = _record_quarantine_entry(
                        state,
                        unreconciled,
                        norm_key,
                        c_name,
                        c_payload,
                        c_act,
                        conflict_entry.get("admission_id"),
                        c_fp,
                        None,
                        "queued_conflict",
                        None,
                        now,
                        save_fp=bool(conflict_entry.get("admitted_without_action_id") or not c_act),
                        replay_fingerprints=c_replay_fps,
                        short_card_id=c_short_id,
                        action_id_inferred=c_inf,
                        delivery_keys=c_dkeys,
                        legacy_headerless=_is_legacy_headerless_entry(conflict_entry, entry_delivery_keys=c_dkeys),
                    )
                    interrupted_items.append((norm_key, c_name, c_payload, c_record))

                if not retained_entry.get("admission_id"):
                    retained_entry["admission_id"] = secrets.token_hex(16)
                    state_dirty = True
                if not retained_entry.get("fingerprint"):
                    retained_entry["fingerprint"] = _entry_fingerprint(norm_key, retained_entry)
                    state_dirty = True
                final_act = _safe_str(retained_entry.get("action_id")) or extract_action_id(retained_entry.get("payload"))
                if final_act and _sync_entry_action_identity(
                    retained_entry,
                    final_act,
                    bool(retained_entry.get("action_id_inferred")),
                ):
                    state_dirty = True
                if isinstance(retained_entry.get("payload"), dict):
                    for tmp_k in ("_admission_id", "_defer_attempts", "_action_id_inferred"):
                        if tmp_k in retained_entry["payload"]:
                            retained_entry["payload"].pop(tmp_k, None)
                            state_dirty = True
                if winner_key != norm_key or len(grouped.get(norm_key, ())) > 1:
                    state_dirty = True
                in_flight[norm_key] = retained_entry
                q_payload = copy.deepcopy(retained_entry.get("payload") or {})
                q_payload["_admission_id"] = retained_entry["admission_id"]
                q_payload["_action_id_inferred"] = bool(retained_entry.get("action_id") and retained_entry.get("action_id_inferred"))
                q_name = retained_entry.get("card_name") or q_payload.get("cardName") or "Unnamed Card"
                queued_items.append((norm_key, q_name, q_payload))

        if len(unreconciled) > 500:
            for old_key in list(unreconciled.keys())[:-500]:
                unreconciled.pop(old_key, None)
            state_dirty = True
        if state_dirty:
            if not save_sidecar_state(state):
                logging.error(
                    "[Trello Sidecar] Failed to persist reconciled in-flight state (%d interrupted).",
                    len(interrupted_items),
                )
                log_sidecar_event("in_flight_reconcile_failed", {"count": len(interrupted_items)})
                return False
        for card_key, _, _ in queued_items:
            pending_cards.add(card_key)

    for card_key, card_name_str, payload, comment_val, list_val in quarantined_queued_logs:
        logging.warning(
            "[Trello Sidecar] Dropping quarantined queued replay for card '%s' (%s) during startup recovery.",
            card_name_str,
            card_key,
        )
        log_trigger_ignored(card_key, card_name_str, payload, comment_val, list_val, "unreconciled_action")

    if queued_items:
        logging.info(f"[Trello Sidecar] Re-enqueueing {len(queued_items)} queued card(s) persisted from previous run.")
    for card_key, card_name_str, payload in queued_items:
        task_queue.put((card_key, card_name_str, payload))
        log_sidecar_event("trigger_requeued", {
            "card_id": card_key,
            "card_name": card_name_str,
            "card_link": payload.get("cardLink"),
        })

    for card_key, card_name_str, payload, record in interrupted_items:
        logging.warning(
            "[Trello Sidecar] Quarantining interrupted %s run for card '%s' (%s) without automatic replay.",
            record["stage"],
            card_name_str,
            card_key,
        )
        log_sidecar_event("trigger_interrupted_unreconciled", {
            "card_id": card_key,
            "card_name": card_name_str,
            "card_link": payload.get("cardLink"),
            "action_id": record.get("action_id"),
            "conversation_id": record.get("conversation_id"),
            "stage": record.get("stage"),
        })
    return True

class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address, RequestHandlerClass, max_workers=None):
        workers = MAX_HTTP_WORKERS if max_workers is None else max_workers
        self._worker_semaphore = threading.BoundedSemaphore(max(1, int(workers)))
        super().__init__(server_address, RequestHandlerClass)

    def process_request(self, request, client_address):
        if not self._worker_semaphore.acquire(blocking=False):
            try:
                body = json.dumps({"error": "Server busy"}).encode("utf-8")
                request.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\n"
                    b"Content-Type: application/json\r\n"
                    + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode("ascii")
                    + body
                )
            except Exception:
                pass
            finally:
                self.shutdown_request(request)
            return
        try:
            t = threading.Thread(target=self._process_request_guarded, args=(request, client_address))
            t.daemon = self.daemon_threads
            t.start()
        except Exception:
            self._worker_semaphore.release()
            self.shutdown_request(request)

    def _process_request_guarded(self, request, client_address):
        try:
            self.process_request_thread(request, client_address)
        finally:
            self._worker_semaphore.release()

class _DeadlineReader:
    def __init__(self, connection, deadline):
        self._conn = connection
        self._deadline = deadline
        self._buf = bytearray()
        self._eof = False

    def _recv_into_buf(self, max_bytes=65536):
        if self._eof:
            return False
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Request read timed out")
        try:
            self._conn.settimeout(remaining)
            chunk = self._conn.recv(max(1, min(65536, int(max_bytes))))
        except socket.timeout as exc:
            raise TimeoutError("Request read timed out") from exc
        if not chunk:
            self._eof = True
            return False
        self._buf.extend(chunk)
        return True

    def readline(self, size=-1):
        limit = None if size is None or size < 0 else int(size)
        if limit == 0:
            return b""
        while True:
            search_end = len(self._buf) if limit is None else min(len(self._buf), limit)
            nl = self._buf.find(b"\n", 0, search_end)
            if nl != -1:
                out = bytes(self._buf[: nl + 1])
                del self._buf[: nl + 1]
                return out
            if limit is not None and len(self._buf) >= limit:
                out = bytes(self._buf[:limit])
                del self._buf[:limit]
                return out
            if self._eof or not self._recv_into_buf():
                out = bytes(self._buf)
                self._buf.clear()
                return out

    def read1(self, size=-1):
        limit = 65536 if size is None or size < 0 else int(size)
        if limit == 0:
            return b""
        while not self._buf and not self._eof:
            if not self._recv_into_buf(limit):
                break
        if not self._buf:
            return b""
        take = min(len(self._buf), limit)
        out = bytes(self._buf[:take])
        del self._buf[:take]
        return out

    def read(self, size=-1):
        if size is None or size < 0:
            while not self._eof:
                self._recv_into_buf()
            out = bytes(self._buf)
            self._buf.clear()
            return out
        limit = int(size)
        if limit <= 0:
            return b""
        while len(self._buf) < limit and not self._eof:
            if not self._recv_into_buf(max(limit - len(self._buf), 4096)):
                break
        take = min(len(self._buf), limit)
        out = bytes(self._buf[:take])
        del self._buf[:take]
        return out

    def close(self):
        return None

def normalize_incoming_webhook(payload, is_native_trello=False):
    work = copy.deepcopy(payload)
    has_action_obj = "action" in work
    has_relay_card_fields = any(
        isinstance(work.get(k), str) and work.get(k).strip()
        for k in ("cardId", "cardidLong", "cardIdLong", "cardLink", "cardShortId")
    )
    treat_as_native = bool(is_native_trello or (has_action_obj and not has_relay_card_fields))

    action_obj = work.get("action")
    if has_action_obj and not isinstance(action_obj, dict):
        return None, None, "ignored_action_type", 200, "Non-object action field"

    raw_data = {}
    raw_card = {}
    raw_action_type = None
    if isinstance(action_obj, dict):
        raw_action_type = _safe_str(action_obj.get("type"))
        if isinstance(action_obj.get("data"), dict):
            raw_data = action_obj["data"]
            if isinstance(raw_data.get("card"), dict):
                raw_card = raw_data["card"]

    top_action_type = _safe_str(work.get("actionType"))
    explicit_action_type = raw_action_type or top_action_type
    if explicit_action_type:
        if explicit_action_type not in RAW_ACTIONABLE_TYPES:
            return None, explicit_action_type, "ignored_action_type", 200, "Ignored action type"
        if explicit_action_type == "addLabelToCard":
            label_obj = raw_data.get("label")
            added_label = (
                (_safe_str(label_obj.get("name")) if isinstance(label_obj, dict) else _safe_str(label_obj))
                or _safe_str(work.get("label"))
                or ""
            )
            if added_label.lower() == "ready for implementation":
                return None, explicit_action_type, "ignored_action_type", 200, "Ignored label action"
    elif treat_as_native:
        return None, None, "ignored_action_type", 200, "Missing action type"

    if treat_as_native:
        raw_cid = raw_card.get("id")
        raw_short = raw_card.get("shortLink")
        if not (
            (isinstance(raw_cid, str) and raw_cid.strip())
            or (isinstance(raw_short, str) and raw_short.strip())
        ):
            return None, explicit_action_type, "missing_card_identity", 200, "Missing card in native action"

    canonical_candidates = []
    for raw_val in (
        raw_card.get("id"),
        work.get("cardidLong"),
        work.get("cardIdLong"),
        work.get("cardId"),
    ):
        norm = normalize_canonical_trello_id(raw_val)
        if norm and norm not in canonical_candidates:
            canonical_candidates.append(norm)
    if len(canonical_candidates) > 1:
        return None, explicit_action_type, "conflicting_card_ids", 400, "Conflicting canonical card IDs"

    if canonical_candidates:
        card_id = canonical_candidates[0]
    else:
        card_id = ""
        for candidate in (
            raw_card.get("shortLink"),
            work.get("cardId") if isinstance(work.get("cardId"), str) else None,
            work.get("cardShortId") if isinstance(work.get("cardShortId"), str) else None,
            extract_short_id(_safe_str(raw_card.get("shortUrl"))),
            extract_short_id(_safe_str(work.get("cardLink"))),
        ):
            if isinstance(candidate, str) and candidate.strip():
                card_id = candidate.strip()
                break

    if not card_id:
        return None, explicit_action_type, "missing_card_identity", 200, "Missing card identity"

    card_name = _safe_str(raw_card.get("name")) or _safe_str(work.get("cardName"))
    raw_card_desc = raw_card.get("desc")
    work_card_desc = work.get("cardDescription")
    card_desc = (
        raw_card_desc
        if isinstance(raw_card_desc, str) and raw_card_desc.strip()
        else (work_card_desc if isinstance(work_card_desc, str) and work_card_desc.strip() else None)
    )
    short_link = _safe_str(raw_card.get("shortLink"))
    card_link = (
        _safe_str(raw_card.get("shortUrl"))
        or (f"https://trello.com/c/{short_link}" if short_link else None)
        or _safe_str(work.get("cardLink"))
    )
    raw_list_after = raw_data.get("listAfter")
    raw_list = raw_data.get("list")
    list_name = (
        (_safe_str(raw_list_after.get("name")) if isinstance(raw_list_after, dict) else _safe_str(raw_list_after))
        or (_safe_str(raw_list.get("name")) if isinstance(raw_list, dict) else _safe_str(raw_list))
        or _safe_str(work.get("listName"))
    )

    if explicit_action_type in NON_COMMENT_ACTION_TYPES:
        comment = ""
        _strip_non_comment_text(work)
    else:
        nested_text = raw_data.get("text") if isinstance(raw_data.get("text"), str) else ""
        top_text = work.get("comment") if isinstance(work.get("comment"), str) else ""
        comment = nested_text if nested_text.strip() else top_text
        if comment:
            work["comment"] = comment
        else:
            work.pop("comment", None)

    work["cardId"] = card_id
    if short_link:
        work["cardShortId"] = short_link
    elif not is_canonical_trello_id(card_id) and not _safe_str(work.get("cardShortId")):
        work["cardShortId"] = card_id
    elif not _safe_str(work.get("cardShortId")):
        work.pop("cardShortId", None)

    for field_name, resolved_val in (
        ("cardName", card_name),
        ("cardDescription", card_desc),
        ("cardLink", card_link),
        ("listName", list_name),
    ):
        if resolved_val:
            work[field_name] = resolved_val
        else:
            work.pop(field_name, None)

    if explicit_action_type and not _safe_str(work.get("actionType")):
        work["actionType"] = explicit_action_type

    return {
        "payload": work,
        "card_id": card_id,
        "card_name": card_name,
        "card_desc": card_desc,
        "card_link": card_link,
        "list_name": list_name,
        "comment": comment,
        "action_type": explicit_action_type,
    }, explicit_action_type, None, 200, None

class WebhookHandler(BaseHTTPRequestHandler):
    def setup(self):
        self.timeout = WEBHOOK_READ_TIMEOUT_SECONDS
        self._request_deadline = time.monotonic() + max(0.05, float(WEBHOOK_READ_TIMEOUT_SECONDS))
        super().setup()
        try:
            self.rfile.close()
        except Exception:
            pass
        self.rfile = _DeadlineReader(self.connection, self._request_deadline)

    def handle_one_request(self):
        self._request_deadline = time.monotonic() + max(0.05, float(WEBHOOK_READ_TIMEOUT_SECONDS))
        if isinstance(getattr(self, "rfile", None), _DeadlineReader):
            self.rfile._deadline = self._request_deadline
        return super().handle_one_request()

    def _send_json(self, status_code, body):
        encoded = json.dumps(body).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _read_request_body(self, content_length):
        deadline = getattr(self, "_request_deadline", None) or (
            time.monotonic() + max(0.05, float(WEBHOOK_READ_TIMEOUT_SECONDS))
        )
        chunks = []
        remaining = content_length
        while remaining > 0:
            time_left = deadline - time.monotonic()
            if time_left <= 0:
                raise TimeoutError("Request body read timed out")
            chunk = self.rfile.read1(min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def do_HEAD(self):
        # Trello sends a HEAD request when verifying the webhook URL
        self.send_response(200)
        self.end_headers()

    def _handle_admin_reconcile(self, payload):
        raw_rec_id = payload.get("recordId") if "recordId" in payload else payload.get("record_id")
        raw_act_id = payload.get("actionId") if "actionId" in payload else payload.get("action_id")
        raw_fp = payload.get("fingerprint")
        for val in (raw_rec_id, raw_act_id, raw_fp):
            if val is not None and not isinstance(val, str):
                self._send_json(400, {"error": "Reconciliation fields must be strings"})
                return
        rec_id = _safe_str(raw_rec_id)
        act_id = _safe_str(raw_act_id)
        fp = _safe_str(raw_fp)
        if not rec_id and not act_id and not fp:
            self._send_json(400, {"error": "Provide recordId, actionId, or fingerprint"})
            return
        status, cleared_ids, remaining_orphans = _reconcile_quarantined_entries(
            action_id=act_id,
            fingerprint=fp,
            record_id=rec_id,
        )
        if status == "ambiguous":
            self._send_json(409, {
                "error": "Ambiguous reconciliation selector matches multiple records; specify recordId",
                "matchingRecordIds": cleared_ids,
            })
            return
        if status != "ok":
            self._send_json(503, {"error": "Failed to persist reconciliation state"})
            return
        resp_body = {"status": "reconciled", "cleared": len(cleared_ids), "recordIds": cleared_ids}
        if remaining_orphans:
            resp_body["remainingLegacyOrphans"] = remaining_orphans
        self._send_json(200, resp_body)

    def do_POST(self):
        if shutdown_event.is_set():
            self.close_connection = True
            self._send_json(503, {"error": "Server shutting down"})
            return

        req_path = urllib.parse.urlsplit(self.path).path.rstrip("/") or "/"
        is_admin_reconcile = (req_path == "/admin/reconcile")

        incoming_token = self.headers.get("X-Auth-Token") or self.headers.get("Authorization")
        trello_sig = self.headers.get("X-Trello-Webhook")
        trello_secret = os.environ.get("TRELLO_SECRET") or TRELLO_SECRET
        has_token_auth = verify_auth_token(incoming_token, AUTH_TOKEN)
        can_check_trello_sig = bool(
            (not is_admin_reconcile) and trello_sig and TRELLO_WEBHOOK_CALLBACK_URL and trello_secret
        )
        if not has_token_auth and not can_check_trello_sig:
            logging.warning("[Trello Sidecar] Unauthorized webhook attempt: Token mismatch or missing.")
            self._send_json(401, {"error": "Unauthorized"})
            return

        raw_length = self.headers.get("Content-Length")
        try:
            content_length = int(raw_length) if raw_length is not None else -1
        except ValueError:
            content_length = -1
        if content_length <= 0:
            self._send_json(400, {"error": "Missing or invalid Content-Length"})
            return
        if content_length > MAX_WEBHOOK_BODY_BYTES:
            self.close_connection = True
            self._send_json(413, {"error": "Payload too large"})
            return

        try:
            post_data = self._read_request_body(content_length)
        except (TimeoutError, socket.timeout):
            self.close_connection = True
            self._send_json(408, {"error": "Request body read timed out"})
            return
        if len(post_data) < content_length:
            self.close_connection = True
            self._send_json(400, {"error": "Incomplete request body"})
            return

        if not has_token_auth:
            if not verify_trello_signature(
                post_data, trello_sig, TRELLO_WEBHOOK_CALLBACK_URL, trello_secret
            ):
                logging.warning("[Trello Sidecar] Unauthorized webhook attempt: Invalid X-Trello-Webhook signature.")
                self._send_json(401, {"error": "Unauthorized"})
                return

        try:
            payload = json.loads(post_data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
            self._send_json(400, {"error": "Invalid JSON payload"})
            return
        if not isinstance(payload, dict):
            self._send_json(400, {"error": "JSON payload must be an object"})
            return

        if is_admin_reconcile:
            self._handle_admin_reconcile(payload)
            return

        try:
            incoming_client_id = (self.headers.get("X-Trello-Client-Identifier") or "").strip()
            if TRELLO_CLIENT_IDENTIFIER and incoming_client_id and incoming_client_id == TRELLO_CLIENT_IDENTIFIER:
                self._send_json(200, {"status": "ignored", "reason": "self_client_identifier"})
                return

            delivery_hdr = (
                _safe_str(self.headers.get("X-Delivery-Id"))
                or _safe_str(self.headers.get("X-Idempotency-Key"))
                or _safe_str(self.headers.get("X-Webhook-Delivery"))
            )
            if delivery_hdr and not extract_relay_delivery_key(payload):
                payload["deliveryId"] = delivery_hdr

            normalized, action_type_norm, err_reason, err_status, err_msg = normalize_incoming_webhook(
                payload,
                is_native_trello=not has_token_auth,
            )
            if normalized is None:
                if err_status != 200:
                    self._send_json(err_status, {"error": err_msg})
                else:
                    log_trigger_ignored(
                        _safe_str(payload.get("cardId")),
                        _safe_str(payload.get("cardName")) or "Unnamed Card",
                        payload,
                        payload.get("comment") if isinstance(payload.get("comment"), str) else "",
                        _safe_str(payload.get("listName")),
                        err_reason,
                    )
                    self._send_json(200, {"status": "ignored", "reason": err_reason})
                return

            payload = normalized["payload"]
            card_id = normalized["card_id"]
            card_name = normalized["card_name"]
            list_name = normalized["list_name"]
            comment = normalized["comment"]
            card_name_str = card_name or "Unnamed Card"

            username, display_name = extract_trigger_member(payload)
            if is_agent_or_suppressed_comment(comment, username):
                log_trigger_ignored(card_id, card_name_str, payload, comment, list_name, "suppressed_trigger_member")
                self._send_json(200, {"status": "ignored", "reason": "suppressed_trigger_member"})
                return

            raw_action_id = extract_action_id(payload)
            pre_enrichment = {
                "raw_action_id": raw_action_id,
                "action_id_inferred": False,
                "raw_username": username,
                "raw_card_id": card_id,
                "raw_short_id": _safe_str(payload.get("cardShortId")) or extract_short_id(_safe_str(payload.get("cardLink"))),
                "verified_short_id": None,
                "payload": copy.deepcopy(payload),
                "comment": comment,
                "list_name": list_name,
            }

            need_canonical_id = not is_canonical_trello_id(card_id)
            need_author = bool(comment and card_id and not username)
            need_action_id = bool(comment and card_id and not raw_action_id and action_type_norm not in NON_COMMENT_ACTION_TYPES)
            need_recent = bool(
                comment
                and card_id
                and not payload.get("recentComments")
                and is_probable_agent_comment(comment, username, display_name)
            )
            if need_canonical_id or need_author or need_action_id or need_recent:
                card_data = fetch_card_details_sync(card_id)
                if card_data:
                    canonical_id = normalize_canonical_trello_id(card_data.get("id")) or _safe_str(card_data.get("id"))
                    if canonical_id:
                        card_id = canonical_id
                    verified_url = _safe_str(card_data.get("url")) or _safe_str(card_data.get("shortUrl"))
                    live_short = _safe_str(card_data.get("shortLink")) or extract_short_id(verified_url)
                    if live_short:
                        pre_enrichment["verified_short_id"] = live_short
                        if not verified_url:
                            verified_url = f"https://trello.com/c/{live_short}"
                        if is_canonical_trello_id(canonical_id):
                            record_verified_card_alias(live_short, canonical_id, card_url=verified_url)
                    _project_verified_card_identity(
                        payload,
                        canonical_id=canonical_id or card_id,
                        verified_short_id=live_short,
                        verified_url=verified_url,
                    )
                    if (need_author or need_action_id) and comment:
                        current_aid = extract_action_id(payload)
                        matched, unique_aid = resolve_comment_match(
                            card_data,
                            comment_text=comment,
                            action_id=current_aid,
                            author_username=username,
                        )
                        if matched:
                            member = matched.get("memberCreator") or {}
                            if isinstance(member, dict):
                                m_user = _safe_str(member.get("username"))
                                m_full = _safe_str(member.get("fullName"))
                                if m_user and not username:
                                    payload["triggerUserUsername"] = m_user.lower()
                                if (m_full or m_user) and not _safe_str(payload.get("triggerUserName")):
                                    payload["triggerUserName"] = m_full or m_user
                            if not current_aid and unique_aid:
                                payload["actionId"] = unique_aid
                                pre_enrichment["action_id_inferred"] = True
                            username, display_name = extract_trigger_member(payload)
                    if (
                        comment
                        and not payload.get("recentComments")
                        and is_probable_agent_comment(comment, username, display_name)
                    ):
                        payload["recentComments"] = extract_recent_comments_from_card_data(
                            card_data,
                            exclude_action_id=extract_action_id(payload),
                        )

            norm_card_id = normalize_canonical_trello_id(card_id)
            if norm_card_id:
                card_id = norm_card_id
                payload["cardId"] = norm_card_id
            else:
                cached_canonical = normalize_canonical_trello_id(lookup_verified_canonical_id(card_id))
                if cached_canonical:
                    if not pre_enrichment.get("verified_short_id") and _safe_str(card_id):
                        pre_enrichment["verified_short_id"] = _safe_str(card_id)
                    card_id = cached_canonical
                    payload["cardId"] = cached_canonical
                else:
                    logging.warning(
                        "[Trello Sidecar] Rejecting trigger for '%s': could not canonicalize card ID %s",
                        card_name_str,
                        card_id,
                    )
                    log_trigger_ignored(
                        card_id,
                        card_name_str,
                        payload,
                        comment,
                        list_name,
                        "canonical_id_resolution_failed",
                    )
                    self._send_json(503, {"error": "Failed to resolve canonical card ID"})
                    return

            if shutdown_event.is_set():
                self.close_connection = True
                self._send_json(503, {"error": "Server shutting down"})
                return

            card_key = card_id
            accepted, reason = should_accept_trigger(
                card_key,
                card_name_str,
                payload,
                comment,
                list_name,
                pre_enrichment=pre_enrichment,
            )
            if not accepted:
                if reason == "state_persistence_failed":
                    self._send_json(503, {"error": "Failed to persist trigger state"})
                else:
                    self._send_json(200, {"status": "ignored", "reason": reason})
                return

            # Enqueue the webhook payload for concurrent background execution
            logging.info(f"[Trello Sidecar] Enqueueing agent trigger for card: '{card_name_str}'")
            task_queue.put((card_key, card_name_str, payload))
            self._send_json(202, {"status": "accepted"})
        except Exception as e:
            logging.error(f"[Trello Sidecar] Error processing webhook: {type(e).__name__}: {redact_sensitive(e)}")
            self._send_json(500, {"error": "Internal Server Error"})

funnel_process = None

def start_tailscale_funnel(port):
    global funnel_process
    if not ENABLE_TAILSCALE_FUNNEL:
        logging.info("Tailscale Funnel disabled (set TRELLO_ENABLE_TAILSCALE_FUNNEL=true to enable).")
        return
    tailscale_bin = "/usr/bin/tailscale"
    
    # 1. Verify that the tailscale binary exists
    if not os.path.exists(tailscale_bin):
        tailscale_bin = shutil.which("tailscale")
        if not tailscale_bin:
            logging.warning(f"Tailscale CLI not found in PATH. Skipping Tailscale Funnel. Server will only run locally on port {port}.")
            return

    # 2. Strict validation: Ensure port is a safe integer
    try:
        port_num = int(port)
        if not (1024 <= port_num <= 65535):
            logging.error(f"Invalid port for Tailscale Funnel: {port_num}")
            return
    except ValueError:
        logging.error(f"Port is not a valid integer: {port}")
        return

    # 3. Secure execution sink with strict allow-list arguments
    logging.info(f"Attempting to start Tailscale Funnel on port {port_num}...")
    try:
        funnel_process = subprocess.Popen(
            [tailscale_bin, "funnel", str(port_num)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        # Bounded check to verify execution started successfully
        time.sleep(1)
        if funnel_process.poll() is not None:
            stdout, stderr = funnel_process.communicate()
            logging.warning(f"Tailscale Funnel failed to start (is it configured/enabled?): {stderr.strip() or stdout.strip()}")
            funnel_process = None
        else:
            logging.info("Tailscale Funnel successfully running in the background.")
    except Exception as e:
        logging.error(f"Error starting Tailscale Funnel process: {e}")
        funnel_process = None

# Graceful signal handler to allow 'finally' blocks to execute on systemd stop
def sig_handler(signum, frame):
    logging.info(f"[Trello Sidecar] Signal {signum} received. Initiating graceful shutdown...")
    raise KeyboardInterrupt()

def run(port=8454, reconcile_retry_delay=0.5):
    # Register SIGTERM/SIGINT handlers
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    reconciled = False
    for attempt in range(3):
        if requeue_in_flight_cards():
            reconciled = True
            break
        if attempt < 2 and reconcile_retry_delay > 0:
            time.sleep(reconcile_retry_delay)
    if not reconciled:
        logging.critical("[Trello Sidecar] Startup state reconciliation failed after 3 attempts; refusing to serve.")
        raise RuntimeError("Startup state reconciliation failed; refusing to serve webhook traffic.")
    global funnel_process
    httpd = None
    try:
        start_queue_worker()

        # Bind only to 127.0.0.1 for secure local-only listening when testing/funneling
        server_address = ('127.0.0.1', port)
        httpd = BoundedThreadingHTTPServer(server_address, WebhookHandler)
        httpd.daemon_threads = True
        logging.info(f"[Trello Sidecar] Server listening on 127.0.0.1:{port}...")

        # Start the Tailscale Funnel in the background
        start_tailscale_funnel(port)

        httpd.serve_forever()
    except KeyboardInterrupt:
        logging.info("[Trello Sidecar] Server shutdown requested.")
    finally:
        if httpd is not None:
            try:
                httpd.server_close()
            except Exception:
                pass
        executor.shutdown(wait=False, cancel_futures=True)

        # Terminate active agent processes before waiting on Funnel exit.
        cleanup_processes()

        # Clean up the funnel process on exit
        if funnel_process:
            logging.info("Terminating Tailscale Funnel process...")
            funnel_process.terminate()
            try:
                funnel_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                logging.info("Forcibly killing Tailscale Funnel process...")
                funnel_process.kill()
            funnel_process = None

if __name__ == '__main__':
    # Listen on port 8454 by default (change via PORT environment variable if needed)
    port_to_use = int(os.environ.get("PORT", "8454"))
    run(port=port_to_use)
