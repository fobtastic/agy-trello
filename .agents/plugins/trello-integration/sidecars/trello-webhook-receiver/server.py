import asyncio
import json
import logging
import os
import queue
import concurrent.futures
import secrets
import shutil
import signal
import subprocess
import threading
import time
import urllib.request
import urllib.parse
import hashlib
import re
from http.server import HTTPServer, BaseHTTPRequestHandler

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

def redact_sensitive(value):
    text = str(value)
    text = re.sub(r'([?&](?:key|token)=)[^&\s]+', r'\1[REDACTED]', text, flags=re.IGNORECASE)
    for secret_value in [
        os.environ.get("TRELLO_API_KEY"),
        os.environ.get("TRELLO_API_TOKEN"),
        os.environ.get("TRELLO_TOKEN"),
        os.environ.get("TRELLO_SECRET"),
        os.environ.get("TRELLO_WEBHOOK_TOKEN"),
    ]:
        if secret_value:
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
        "fields": "name,desc,closed,idList,url,labels,due,dateLastActivity"
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
        logging.error(f"[Trello Sidecar] Exception fetching card details: {redact_sensitive(e)}")
    return None

def post_trello_comment(card_id, text):
    api_key = os.environ.get("TRELLO_API_KEY")
    api_token = os.environ.get("TRELLO_API_TOKEN") or os.environ.get("TRELLO_TOKEN")
    if not api_key or not api_token or not card_id:
        return
    url = f"https://api.trello.com/1/cards/{trello_path_id(card_id)}/actions/comments?key={api_key}&token={api_token}"
    data = {"text": text}
    encoded_data = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, method="POST", data=encoded_data)
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            logging.info(f"[Trello Sidecar] Posted acknowledgement comment on Trello card {card_id}")
    except Exception as e:
        logging.error(f"[Trello Sidecar] Failed to post acknowledgement comment: {redact_sensitive(e)}")

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
AGENT_COMMENT_MARKER = os.environ.get("TRELLO_AGENT_COMMENT_MARKER", "[agy-sidecar:comment]")
AGENT_ACK_MARKER = os.environ.get("TRELLO_AGENT_ACK_MARKER", "[agy-sidecar:ack]")
AGENT_STATUS_MARKER = os.environ.get("TRELLO_AGENT_STATUS_MARKER", "[agy-sidecar:status]")
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

# 3. Active Process Tracking for Graceful Cleanup
active_processes = set()

def cleanup_processes():
    if not active_processes:
        return
    logging.info(f"[Trello Sidecar] Cleaning up {len(active_processes)} active agent processes...")
    for proc in list(active_processes):
        try:
            logging.info(f"[Trello Sidecar] Terminating agent process {proc.pid}...")
            proc.terminate()
        except Exception as e:
            logging.warning(f"[Trello Sidecar] Failed to terminate process {proc.pid}: {e}")

# 4. Session/Conversation Tracking (Resuming specific card conversations)
SESSION_MAP_FILE = os.path.expanduser("~/.gemini/antigravity-cli/trello_sessions.json")
SIDECAR_STATE_FILE = os.path.expanduser("~/.gemini/antigravity-cli/trello_sidecar_state.json")
state_lock = threading.Lock()
pending_cards = set()

def load_session_mapping():
    if os.path.exists(SESSION_MAP_FILE):
        try:
            with open(SESSION_MAP_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"[Trello Sidecar] Error loading session mapping: {e}")
    return {}

def save_session_mapping(mapping):
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(SESSION_MAP_FILE), exist_ok=True)
        with open(SESSION_MAP_FILE, 'w') as f:
            json.dump(mapping, f, indent=2)
    except Exception as e:
        logging.error(f"[Trello Sidecar] Error saving session mapping: {e}")

def load_sidecar_state():
    if os.path.exists(SIDECAR_STATE_FILE):
        try:
            with open(SIDECAR_STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"[Trello Sidecar] Error loading sidecar state: {e}")
    return {"recent_triggers": [], "card_activity": {}}

def save_sidecar_state(state):
    try:
        os.makedirs(os.path.dirname(SIDECAR_STATE_FILE), exist_ok=True)
        with open(SIDECAR_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logging.error(f"[Trello Sidecar] Error saving sidecar state: {e}")

def find_new_conversation_id(before_dirs):
    brain_dir = os.path.expanduser("~/.gemini/antigravity-cli/brain")
    if not os.path.exists(brain_dir):
        return None
    try:
        after_dirs = set(os.listdir(brain_dir))
        new_dirs = after_dirs - before_dirs
        for d in new_dirs:
            # Check if it is a directory
            if os.path.isdir(os.path.join(brain_dir, d)):
                return d
    except Exception as e:
        logging.error(f"[Trello Sidecar] Error scanning brain directory: {e}")
    return None

async def classify_action(comment, card_name, card_desc, list_name):
    """Performs a quick classification run using Gemini 3.5 Flash to determine the card's phase semantically."""
    # Direct list name mapping
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

    cmd = [
        resolve_agy_bin(),
        "--dangerously-skip-permissions",
        "--model", "Gemini 3.5 Flash (Medium)",
        "--print",
        f"Classify the following Trello trigger semantically to determine the expected phase.\n\n"
        f"Card Name: {card_name}\n"
        f"Card Description: {card_desc}\n"
        f"List Name: {list_name}\n"
        f"Triggering Comment: {comment}\n\n"
        f"Respond with exactly one word (no punctuation, no explanation, in uppercase):\n"
        f"- READY_FOR_SPEC (if the list name is 'Ready for Spec' or 'Ready for Specification', or if the comment or description indicates requirements are finalized, locked in, ready to build/spec, or requests writing/creating a spec or GitHub issues)\n"
        f"- INVESTIGATOR (if the list name is 'Ideas', 'Research', 'In Progress', or if the comment or card status asks for reviews, architectural suggestions, feedback, options, or is an early stage description that needs refinement/information)\n"
        f"- GENERAL (if the trigger is conversational, generic testing, status updates, asks a simple follow-up question, asks for clarification on existing work, or is discussion not requesting a new spec/GitHub issue)"
    ]
    logging.info(f"[Trello Sidecar] Classifying trigger comment semantically...")
    try:
        target_cwd = WORKSPACES[0] if WORKSPACES else None
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=target_cwd
        )
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            result = stdout.decode().strip().upper()
            logging.info(f"[Trello Sidecar] Semantic classification raw output: '{result}'")
            if "READY_FOR_SPEC" in result:
                return "READY_FOR_SPEC"
            elif "INVESTIGATOR" in result:
                return "INVESTIGATOR"
            else:
                return "GENERAL"
        else:
            logging.error(f"[Trello Sidecar] Classification process failed with code {process.returncode}: {stderr.decode()}")
    except Exception as e:
        logging.error(f"[Trello Sidecar] Exception during classification: {e}")
    return "GENERAL"  # Fallback to general discussion

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
    action_data = extract_action(payload)
    member_creator = action_data.get("memberCreator")
    if isinstance(member_creator, dict):
        username = member_creator.get("username")
        name = member_creator.get("fullName") or username
        return (username or "").strip().lower(), name or "Unknown"
    username = payload.get("triggerUserUsername")
    name = payload.get("triggerUserName") or username or "Unknown"
    return (username or "").strip().lower(), name

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

def extract_action_id(payload):
    action_data = extract_action(payload)
    return action_data.get("id") or payload.get("actionId") or payload.get("idAction")

def extract_action_type(payload):
    action_data = extract_action(payload)
    return action_data.get("type") or payload.get("actionType") or "unknown"

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
    for marker in [AGENT_COMMENT_MARKER, AGENT_ACK_MARKER, AGENT_STATUS_MARKER]:
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
    if any(marker and marker in text for marker in [AGENT_COMMENT_MARKER, AGENT_ACK_MARKER, AGENT_STATUS_MARKER]):
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

def should_post_ack_comment(card_key, phase, state, now=None):
    if phase not in {"INVESTIGATOR", "READY_FOR_SPEC"}:
        return False
    now = time.time() if now is None else now
    last_ack_by_card = state.setdefault("last_ack_by_card", {})
    last_ack = float(last_ack_by_card.get(str(card_key), 0) or 0)
    return now - last_ack >= ACK_COOLDOWN_SECONDS

def record_ack_comment(card_key):
    with state_lock:
        state = load_sidecar_state()
        state.setdefault("last_ack_by_card", {})[str(card_key)] = time.time()
        save_sidecar_state(state)

def reserve_ack_comment(card_key, phase):
    if not POST_ACK_COMMENTS:
        return False
    with state_lock:
        state = load_sidecar_state()
        if not should_post_ack_comment(card_key, phase, state):
            return False
        state.setdefault("last_ack_by_card", {})[str(card_key)] = time.time()
        save_sidecar_state(state)
        return True

def trigger_fingerprint(card_key, payload, comment, list_name):
    parts = [
        str(card_key or ""),
        extract_action_type(payload),
        normalize_trigger_text(comment),
        normalize_trigger_text(list_name),
        normalize_trigger_text(payload.get("cardName")),
    ]
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()

def is_agent_or_suppressed_comment(comment, username):
    if username and username in SUPPRESSED_TRELLO_USERNAMES:
        return True
    if not comment:
        return False
    if any(marker and marker in comment for marker in [AGENT_ACK_MARKER, AGENT_STATUS_MARKER]):
        return True
    try:
        return bool(re.search(SUPPRESSED_COMMENT_REGEX, comment))
    except re.error as e:
        logging.warning("[Trello Sidecar] Invalid TRELLO_SUPPRESSED_COMMENT_REGEX: %s", e)
        return f"- Love {AGENT_SIGNATURE_NAME}" in comment

def should_accept_trigger(card_key, card_name, payload, comment, list_name):
    now = time.time()
    username, display_name = extract_trigger_member(payload)
    if is_agent_or_suppressed_comment(comment, username):
        logging.info(
            "[Trello Sidecar] Ignoring trigger from suppressed member/signature on card '%s' (member=%s)",
            card_name,
            username or display_name,
        )
        return False, "suppressed_trigger_member"
    if is_probable_agent_comment(comment, username, display_name):
        recent_comments = extract_recent_comment_texts(payload)
        if is_low_signal_agent_comment(comment, recent_comments):
            logging.info(
                "[Trello Sidecar] Ignoring low-novelty agent comment on card '%s' (member=%s).",
                card_name,
                username or display_name,
            )
            return False, "low_novelty_agent_comment"

    action_id = extract_action_id(payload)
    fingerprint = trigger_fingerprint(card_key, payload, comment, list_name)

    with state_lock:
        if card_key in pending_cards:
            logging.info("[Trello Sidecar] Ignoring duplicate trigger while card '%s' already has a queued/running agent.", card_name)
            return False, "card_already_running"

        state = load_sidecar_state()
        recent_triggers = state.setdefault("recent_triggers", [])
        recent_ids = {entry.get("id") for entry in recent_triggers if entry.get("id")}
        if action_id and action_id in recent_ids:
            logging.info("[Trello Sidecar] Ignoring already-seen Trello action %s for card '%s'.", action_id, card_name)
            return False, "duplicate_action"

        card_activity = state.setdefault("card_activity", {})
        last = card_activity.get(str(card_key), {})
        last_seen = float(last.get("last_seen", 0) or 0)
        if last.get("fingerprint") == fingerprint and now - last_seen < TRIGGER_COOLDOWN_SECONDS:
            logging.info(
                "[Trello Sidecar] Ignoring repeated trigger fingerprint for card '%s' within %ss cooldown.",
                card_name,
                TRIGGER_COOLDOWN_SECONDS,
            )
            return False, "trigger_cooldown"

        if action_id:
            recent_triggers.append({"id": action_id, "card": card_key, "seen_at": now})
            state["recent_triggers"] = recent_triggers[-MAX_RECENT_TRIGGERS:]
        card_activity[str(card_key)] = {
            "fingerprint": fingerprint,
            "last_seen": now,
            "last_action_id": action_id,
        }
        pending_cards.add(card_key)
        save_sidecar_state(state)

    return True, "accepted"

def release_pending_card(card_key):
    with state_lock:
        pending_cards.discard(card_key)

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

def resolve_conversation_alias(session_map, card_key, card_name, card_link):
    aliases = []
    for value in [card_key, extract_short_id(card_link), card_name]:
        if value and value not in aliases:
            aliases.append(value)

    for alias in aliases:
        conversation_id = session_map.get(alias)
        if conversation_id:
            for other_alias in aliases:
                if other_alias and session_map.get(other_alias) != conversation_id:
                    session_map[other_alias] = conversation_id
            return conversation_id, aliases
    return None, aliases

def collect_previous_conversation_context(conversation_id, max_chars=7000):
    if not conversation_id:
        return ""
    brain_path = os.path.expanduser(f"~/.gemini/antigravity-cli/brain/{conversation_id}")
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

async def trigger_agent(card_key, card_name, payload):
    """Spawns an Antigravity agent by executing the agy CLI binary as a subprocess."""
    # Extract fields needed for classification
    comment = payload.get("comment", "")
    card_desc = payload.get("cardDescription", "")
    list_name = payload.get("listName", "")
    
    # Try to fetch live card details from Trello
    card_id = payload.get("cardId")
    if not card_id and payload.get("cardLink"):
        card_id = extract_short_id(payload.get("cardLink"))
        if card_id:
            payload["cardId"] = card_id
    card_details_str = ""
    latest_comment_action = None
    if card_id:
        loop = asyncio.get_running_loop()
        card_data = await loop.run_in_executor(None, fetch_card_details_sync, card_id)
        if card_data:
            card_details_str = format_card_details(card_data)
            # Override local variables with latest live data from Trello
            card_name = card_data.get("name", card_name)
            card_desc = card_data.get("desc", card_desc)
            if card_data.get("list"):
                list_name = card_data["list"].get("name", list_name)
            
            # If no triggering comment is provided in payload, fall back to the latest comment on the card
            actions = card_data.get("actions", [])
            comment_actions = [a for a in actions if a.get("type") == "commentCard"]
            if comment_actions:
                # Sort comment actions by date descending to find the newest
                comment_actions.sort(key=lambda x: x.get("date", ""), reverse=True)
                latest_comment_action = comment_actions[0]
                if not comment:
                    comment = latest_comment_action.get("data", {}).get("text", "")

    # Resolve the triggering Trello member if available
    trigger_username = None
    trigger_name = "Unknown"

    # 1. Try extracting from raw webhook action
    action_data = payload.get("action")
    if isinstance(action_data, dict):
        member_creator = action_data.get("memberCreator")
        if isinstance(member_creator, dict):
            trigger_username = member_creator.get("username")
            trigger_name = member_creator.get("fullName") or trigger_username

    # 2. Try the latest comment action from live fetched card data
    if not trigger_username and latest_comment_action:
        member_creator = latest_comment_action.get("memberCreator")
        if isinstance(member_creator, dict):
            trigger_username = member_creator.get("username")
            trigger_name = member_creator.get("fullName") or trigger_username

    # 3. Fallback/override check in case payload is simplified and has direct user fields
    if not trigger_username:
        trigger_username = payload.get("triggerUserUsername")
    if trigger_name == "Unknown":
        trigger_name = payload.get("triggerUserName", "Unknown")

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
    phase = await classify_action(comment, card_name, card_desc, list_name)
    logging.info(f"[Trello Sidecar] Determined phase: {phase} for card '{card_name}'")
    if card_id and reserve_ack_comment(card_key, phase):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, post_trello_comment, card_id, build_ack_comment())
    github_work_context = collect_github_work_context(card_name, payload.get("cardLink")) if phase == "READY_FOR_SPEC" else ""
    stakeholder_context = build_stakeholder_context_block(trigger_username)
    
    # Choose the model and configure system instructions based on the phase
    if phase == "READY_FOR_SPEC":
        model_name = "Gemini 3.1 Pro (High)"
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
            "   - Call the `call_mcp_tool` tool with parameters exactly shaped as: `ServerName: \"codex-mcp\"`, `ToolName: \"codex\"`, and `Arguments: {\"model\": \"gpt-5.5\", \"config\": {\"model_reasoning_effort\": \"high\"}, \"prompt\": \"[bounded review prompt]\"}`. The `Arguments` object must contain a top-level `prompt` field.\n"
            "   - Refine and adjust your specification based on Codex's feedback/critique before proceeding.\n"
            "6. Create matching issues in the appropriate Frontend (FE) and Backend (BE) Github repositories only after the duplicate/progress check passes. The `gh` CLI is installed and pre-authenticated for this runtime environment. Use `gh issue create --repo <owner/repo> --title \"Title\" --body \"Body\"` instead of writing custom API scripts, and relate the issues to each other. **CRITICAL GUARDRAIL:** The body of every created GitHub issue MUST include a clear, direct link back to the originating Trello card (the Trello card link can be found in the webhook trigger payload as `cardLink` or in the card details as `url`, e.g., `https://trello.com/c/...`). This ensures context is not lost and allows status updates/syncing later.\n"
            "7. Link the created or reused GitHub issues/PRs back to the Trello card.\n"
            "8. Remove the 'Ready for Spec' label on the Trello card, add the 'Ready for Implementation' label, and move the card to the 'Ready for Implementation' list only after the GitHub work item set is confirmed non-duplicative.\n"
            "9. Relate specs to each other as appropriate, especially if there's both a FE and BE ticket as a result of the request.\n"
            "10. **Preserve QA/Discussion Context**: Since the QA, investigation, and design alignment conversations occurred asynchronously without engineering in the loop, you must summarize this context in your final specification. Detail what was asked during the grilling/QA phase, why it was asked, and what specific decision or option the PM/designer selected.\n"
            "11. Address your response/comments to the triggering comment author and relevant Trello stakeholders. Honor configured mention policies and never @-mention the acting/posting account or usernames marked `never_at_mention`. "
            f"Sign any card updates/comments with \"- Love {AGENT_SIGNATURE_NAME}\".\n\n"
            "### Trello Helper Utility (MANDATORY)\n"
            "You MUST use the pre-installed CLI utility via run_command for ALL Trello operations (comment, move, add-label, remove-label). Do NOT construct raw `curl` commands, do NOT use inline HTTP request scripts, and do NOT write custom python files for Trello API calls. You must invoke the helper exactly as follows:\n"
            f"- Comment: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py comment <card_id> \"<text>\"`\n"
            f"- Move to List: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py move <card_id> \"<list_name>\"`\n"
            f"- Add Label: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py add-label <card_id> \"<label_name>\"`\n"
            f"- Remove Label: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py remove-label <card_id> \"<label_name>\"`\n\n"
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
        model_name = "Gemini 3.1 Pro (High)"
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
            "### Trello Helper Utility (MANDATORY)\n"
            "You MUST use the pre-installed CLI utility via run_command for ALL Trello operations (comment, move, add-label, remove-label). Do NOT construct raw `curl` commands, do NOT use inline HTTP request scripts, and do NOT write custom python files for Trello API calls. You must invoke the helper exactly as follows:\n"
            f"- Comment: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py comment <card_id> \"<text>\"`\n"
            f"- Move to List: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py move <card_id> \"<list_name>\"`\n"
            f"- Add Label: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py add-label <card_id> \"<label_name>\"`\n"
            f"- Remove Label: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py remove-label <card_id> \"<label_name>\"`\n"
        )
    else:
        model_name = "Gemini 3.5 Flash (Medium)"
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
            "### Trello Helper Utility (MANDATORY)\n"
            "You MUST use the pre-installed CLI utility via run_command for ALL Trello operations (comment, move, add-label, remove-label). Do NOT construct raw `curl` commands, do NOT use inline HTTP request scripts, and do NOT write custom python files for Trello API calls. You must invoke the helper exactly as follows:\n"
            f"- Comment: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py comment <card_id> \"<text>\"`\n"
            f"- Move to List: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py move <card_id> \"<list_name>\"`\n"
            f"- Add Label: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py add-label <card_id> \"<label_name>\"`\n"
            f"- Remove Label: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py remove-label <card_id> \"<label_name>\"`\n"
        )

    # Load existing session map
    session_map = load_session_mapping()
    card_link = payload.get("cardLink")
    conversation_id, conversation_aliases = resolve_conversation_alias(session_map, card_key, card_name, card_link)
    if conversation_id:
        logging.info(
            "[Trello Sidecar] Found session %s for card '%s' via aliases: %s",
            conversation_id,
            card_name,
            conversation_aliases,
        )
        save_session_mapping(session_map)
    previous_conversation_context = collect_previous_conversation_context(conversation_id)
    
    # Keep track of brain directories before launch to capture new UUIDs
    brain_dir = os.path.expanduser("~/.gemini/antigravity-cli/brain")
    before_dirs = set(os.listdir(brain_dir)) if os.path.exists(brain_dir) else set()
    
    # Build command to execute agy CLI with auto-approval for tool execution permissions
    cmd = [
        resolve_agy_bin(),
        "--dangerously-skip-permissions",
        "--print-timeout", "15m",
        "--model", model_name
    ]
    
    # Add workspaces/directories if specified
    if WORKSPACES:
        for ws in WORKSPACES:
            cmd.extend(["--add-dir", ws])
            
    # Add conversation resume flag if available
    if conversation_id:
        logging.info(f"[Trello Sidecar] Resuming existing conversation {conversation_id} for card '{card_name}' using model '{model_name}'")
        cmd.extend(["--conversation", conversation_id])
    else:
        logging.info(f"[Trello Sidecar] Starting new conversation thread for card '{card_name}' using model '{model_name}'")
            
    payload_str = json.dumps(payload, indent=2)
    
    security_rules = (
        "CRITICAL SECURITY REQUIREMENT:\n"
        "- Do NOT print, comment, or reveal any credentials, API keys, secrets, tokens, or passwords under any circumstances. "
        "This applies to Trello tokens/keys, database passwords (like db_reader), .env file contents, or any frontend/backend configuration keys. "
        "If referring to configuration state, only indicate presence (e.g. 'verified TRELLO_API_KEY is configured') and NEVER print/disclose the actual values.\n"
        "- STRICT READ-ONLY GUARDRAIL: You are strictly in an investigation and planning phase. You are STRICTLY FORBIDDEN from editing, creating, or deleting any files in the workspace repositories. "
        "Do NOT use tools like write_to_file, replace_file_content, multi_replace_file_content, or run commands that modify code. Base your specifications and options purely on reading the code."
    )
    
    prompt = (
        f"Role & Core Instructions:\n{system_instruction}\n\n"
        f"Security Rules:\n{security_rules}\n\n"
    )
    if trigger_user_info:
        prompt += trigger_user_info
    if stakeholder_context:
        prompt += stakeholder_context
    if workspaces_info:
        prompt += (
            f"--- Active Workspaces ---\n"
            f"{workspaces_info}\n"
            f"-------------------------\n\n"
        )
    if github_work_context:
        prompt += github_work_context
    if previous_conversation_context:
        prompt += previous_conversation_context
    if card_details_str:
        prompt += (
            f"--- Trello Card Live State ---\n"
            f"{card_details_str}\n"
            f"------------------------------\n\n"
        )
    prompt += (
        f"--- Webhook Trigger Payload ---\n"
        f"{payload_str}\n"
        f"-------------------------------\n\n"
        f"1. Examine the Trello webhook trigger payload and the live card state above to understand the current Trello card context (cardName, cardDescription, listName, checklists, comments, and recent activities).\n"
        f"2. Execute your specialized mode behavior (PLANNER, INVESTIGATOR, or GENERAL discussion) based on the payload and card state.\n"
        f"3. Output your response or spec summary."
    )
    
    cmd.extend(["--print", prompt])
    
    logging.info(f"[Trello Sidecar] Running agy command: {format_command_for_log(cmd)}")
    
    target_cwd = WORKSPACES[0] if WORKSPACES else None
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=target_cwd
        )
        active_processes.add(process)
        
        # If starting a new session, discover and log the new conversation ID after launch
        if not conversation_id:
            for _ in range(15):
                await asyncio.sleep(1)
                new_id = find_new_conversation_id(before_dirs)
                if new_id:
                    logging.info(f"[Trello Sidecar] Discovered new conversation ID {new_id} for card '{card_name}'")
                    for alias in conversation_aliases:
                        if alias:
                            session_map[alias] = new_id
                    save_session_mapping(session_map)
                    break
        
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            logging.info(f"[Trello Sidecar] Agent task completed successfully for card '{card_name}'")
            logging.info(f"[Trello Sidecar] Output:\n{stdout.decode()}")
        else:
            logging.error(f"[Trello Sidecar] Agent process exited with code {process.returncode} for card '{card_name}'")
            logging.error(f"[Trello Sidecar] Stderr:\n{stderr.decode()}")
            raise RuntimeError(f"Agent failed with code {process.returncode}")
    except Exception as e:
        logging.error(f"[Trello Sidecar] Error executing agy CLI subprocess: {e}")
        raise
    finally:
        if process and process in active_processes:
            active_processes.remove(process)

# Create a thread-safe queue for Trello agent triggers
task_queue = queue.Queue()

# ThreadPoolExecutor to run up to 3 agents concurrently
executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
queue_worker_started = False

def run_agent_job(card_key, card_name_str, payload):
    logging.info(f"[Trello Sidecar] Worker thread starting execution for card: '{card_name_str}'")
    try:
        asyncio.run(trigger_agent(card_key, card_name_str, payload))
    except Exception as e:
        logging.error(f"[Trello Sidecar] Exception during concurrent agent execution: {e}")
    finally:
        release_pending_card(card_key)

def queue_worker():
    logging.info("[Trello Sidecar] Background queue worker thread started successfully.")
    while True:
        try:
            card_key, card_name_str, payload = task_queue.get()
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

class WebhookHandler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        # Trello sends a HEAD request when verifying the webhook URL
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        # Validate incoming token via header (X-Auth-Token or Authorization)
        incoming_token = self.headers.get("X-Auth-Token") or self.headers.get("Authorization")
        if not incoming_token or incoming_token != AUTH_TOKEN:
            logging.warning("[Trello Sidecar] Unauthorized webhook attempt: Token mismatch or missing.")
            self.send_response(401)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"error": "Unauthorized"}')
            return

        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        
        try:
            payload = json.loads(post_data.decode('utf-8'))
            
            # Extract card info (supporting both Trello raw webhook and simplified schema)
            card_id = payload.get("cardId")
            if not card_id or not str(card_id).strip():
                card_id = payload.get("cardidLong") or payload.get("cardIdLong") or payload.get("cardId")
            card_name = payload.get("cardName")
            card_desc = payload.get("cardDescription")
            card_link = payload.get("cardLink")
            list_name = payload.get("listName")
            comment = payload.get("comment")
            
            # Fallback for raw Trello webhook structure
            if "action" in payload:
                action_data = payload.get("action", {})
                card_data = action_data.get("data", {}).get("card", {})
                if card_data:
                    card_id = card_data.get("id") or card_id
                    card_name = card_data.get("name") or card_name
                    card_desc = card_data.get("desc") or card_desc
                    card_link = card_data.get("shortUrl") or card_link
                list_data = action_data.get("data", {}).get("list", {})
                if list_data:
                    list_name = list_data.get("name") or list_name
                if action_data.get("type") == "commentCard":
                    comment = action_data.get("data", {}).get("text") or comment

            if (not card_id or not str(card_id).strip()) and card_link:
                card_id = extract_short_id(card_link) or card_id

            card_key = card_id or card_name or "Unnamed Card"
            card_name_str = card_name or "Unnamed Card"

            # Ensure the resolved card ID is injected back into the payload so the agent can use it
            if card_id:
                payload["cardId"] = card_id
            if card_name:
                payload["cardName"] = card_name
            if card_desc:
                payload["cardDescription"] = card_desc
            if card_link:
                payload["cardLink"] = card_link
            if list_name:
                payload["listName"] = list_name
            if comment:
                payload["comment"] = comment

            username, display_name = extract_trigger_member(payload)
            if (
                comment
                and card_id
                and not payload.get("recentComments")
                and not is_agent_or_suppressed_comment(comment, username)
                and is_probable_agent_comment(comment, username, display_name)
            ):
                card_data = fetch_card_details_sync(card_id)
                if card_data:
                    payload["recentComments"] = extract_recent_comments_from_card_data(
                        card_data,
                        exclude_action_id=extract_action_id(payload),
                    )

            accepted, reason = should_accept_trigger(card_key, card_name_str, payload, comment, list_name)
            if not accepted:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ignored", "reason": reason}).encode("utf-8"))
                return
            
            # Enqueue the webhook payload for concurrent background execution
            logging.info(f"[Trello Sidecar] Enqueueing agent trigger for card: '{card_name_str}'")
            task_queue.put((card_key, card_name_str, payload))
            
            self.send_response(202)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"accepted"}')
        except Exception as e:
            # TODO(security): Log detailed diagnostic info locally, and return generic error message
            logging.error(f"[Trello Sidecar] Error processing webhook: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"error": "Internal Server Error"}')

funnel_process = None

def start_tailscale_funnel(port):
    global funnel_process
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

def run(port=8454):
    # Register SIGTERM/SIGINT handlers
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    start_queue_worker()

    # Bind only to 127.0.0.1 for secure local-only listening when testing/funneling
    server_address = ('127.0.0.1', port)
    httpd = HTTPServer(server_address, WebhookHandler)
    logging.info(f"[Trello Sidecar] Server listening on 127.0.0.1:{port}...")
    
    # Start the Tailscale Funnel in the background
    start_tailscale_funnel(port)
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logging.info("[Trello Sidecar] Server shutdown requested.")
    finally:
        # Clean up the funnel process on exit
        global funnel_process
        if funnel_process:
            logging.info("Terminating Tailscale Funnel process...")
            funnel_process.terminate()
            try:
                funnel_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                logging.info("Forcibly killing Tailscale Funnel process...")
                funnel_process.kill()
        
        # Terminate active agent processes
        cleanup_processes()

if __name__ == '__main__':
    # Listen on port 8454 by default (change via PORT environment variable if needed)
    port_to_use = int(os.environ.get("PORT", "8454"))
    run(port=port_to_use)
