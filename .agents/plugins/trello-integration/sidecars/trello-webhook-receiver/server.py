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
    url = f"https://api.trello.com/1/cards/{card_id}?" + urllib.parse.urlencode(params)
    
    logging.info(f"[Trello Sidecar] Fetching card details from Trello for card ID: {card_id}...")
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status == 200:
                return json.loads(response.read().decode('utf-8'))
            else:
                logging.error(f"[Trello Sidecar] Failed to fetch card details. HTTP Status: {response.status}")
    except Exception as e:
        logging.error(f"[Trello Sidecar] Exception fetching card details: {e}")
    return None

def post_trello_comment(card_id, text):
    api_key = os.environ.get("TRELLO_API_KEY")
    api_token = os.environ.get("TRELLO_API_TOKEN") or os.environ.get("TRELLO_TOKEN")
    if not api_key or not api_token or not card_id:
        return
    url = f"https://api.trello.com/1/cards/{card_id}/actions/comments?key={api_key}&token={api_token}"
    data = {"text": text}
    encoded_data = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, method="POST", data=encoded_data)
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            logging.info(f"[Trello Sidecar] Posted acknowledgement comment on Trello card {card_id}")
    except Exception as e:
        logging.error(f"[Trello Sidecar] Failed to post acknowledgement comment: {e}")

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
AGENT_SIGNATURE_NAME = os.environ.get("TRELLO_AGENT_SIGNATURE_NAME", "Agy")

def resolve_agy_bin():
    agy_bin = shutil.which("agy")
    if agy_bin:
        return agy_bin
    local_agy = os.path.expanduser("~/.local/bin/agy")
    if os.path.exists(local_agy):
        return local_agy
    return "/home/ubuntu/.local/bin/agy"

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
        f"- GENERAL (if the trigger is conversational, generic testing, status updates, or general discussion/questions not related to planning or building a specific feature)"
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
    import re
    match = re.search(r'/c/([a-zA-Z0-9]+)', card_link)
    if match:
        return match.group(1)
    return None

def resolve_git_repo(directory):
    try:
        import re
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

async def trigger_agent(card_key, card_name, payload):
    """Spawns an Antigravity agent by executing the agy CLI binary as a subprocess."""
    # Extract fields needed for classification
    comment = payload.get("comment", "")
    card_desc = payload.get("cardDescription", "")
    list_name = payload.get("listName", "")
    
    # Try to fetch live card details from Trello
    card_id = payload.get("cardId")
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

    trigger_user_info = ""
    if trigger_username:
        trigger_user_info = (
            f"--- Webhook Triggering User ---\n"
            f"This action/discussion was triggered by the following Trello member:\n"
            f"- Name: {trigger_name}\n"
            f"- Trello Username: @{trigger_username}\n"
            f"When replying on the card, direct your response/tag to this user and other relevant stakeholders. "
            f"Do NOT tag @fobtastic in your comment, as you are acting on behalf of @fobtastic.\n"
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
    
    # Choose the model and configure system instructions based on the phase
    if phase == "READY_FOR_SPEC":
        model_name = "Gemini 3.1 Pro (High)"
        system_instruction = (
            "You are reviewing and responding to Trello cards as the PLANNER on behalf of @fobtastic (Chris Tou). "
            "Use the Trello API to interact with the specific card that triggered you (Trello credentials are available in the environment variables: "
            "TRELLO_API_KEY, TRELLO_SECRET, and TRELLO_API_TOKEN).\n\n"
            "This card is Ready for Spec (finalized/locked in). Your goal is to write a detailed specification and create GitHub issues:\n"
            "1. Create a detailed spec based on the title, description, and discussions in the card. Ensure it is grounded in the existing codebase.\n"
            "2. Once you write the draft plan, you must request an adversarial second opinion/review from Codex using the Codex MCP server:\n"
            "   - Call the `call_mcp_tool` tool with parameters: `ServerName: \"codex-mcp\"`, `ToolName: \"codex\"`, and `Arguments: {\"model\": \"gpt-5.5\", \"config\": {\"model_reasoning_effort\": \"high\"}, \"prompt\": \"Please review this proposed implementation plan and provide an adversarial second opinion. Highlight potential flaws, edge cases, or optimizations. Here is the draft plan: [Insert your draft plan] and the initial Trello request context: [Insert initial request/discussions].\"}`.\n"
            "   - Refine and adjust your specification based on Codex's feedback/critique before proceeding.\n"
            "3. Create matching issues in the appropriate Frontend (FE) and Backend (BE) Github repositories. The `gh` CLI is installed and pre-authenticated for user @fobtastic. Use `gh issue create --repo <owner/repo> --title \"Title\" --body \"Body\"` instead of writing custom API scripts, and relate the issues to each other.\n"
            "4. Link the created GitHub issues back to the Trello card.\n"
            "5. Remove the 'Ready for Spec' label on the Trello card, add the 'Ready for Implementation' label, and move the card to the 'Ready for Implementation' list.\n"
            "6. Relate specs to each other as appropriate, especially if there's both a FE and BE ticket as a result of the request.\n"
            "7. **Preserve QA/Discussion Context**: Since the QA, investigation, and design alignment conversations occurred asynchronously without engineering in the loop, you must summarize this context in your final specification. Detail what was asked during the grilling/QA phase, why it was asked, and what specific decision or option the PM/designer selected.\n"
            "8. Address your response/comments to the relevant Trello members/stakeholders (do NOT tag @fobtastic since you are acting on behalf of this account). "
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
        )
    elif phase == "INVESTIGATOR":
        model_name = "Gemini 3.1 Pro (High)"
        system_instruction = (
            "You are reviewing and responding to Trello cards as the INVESTIGATOR on behalf of @fobtastic (Chris Tou). "
            "Use the Trello API to interact with the specific card that triggered you (Trello credentials are available in the environment variables: "
            "TRELLO_API_KEY, TRELLO_SECRET, and TRELLO_API_TOKEN).\n\n"
            "Use the grill-me skill to align on requirements and resolve design decisions through a structured, interactive interview tailored for Trello's asynchronous nature:\n"
            "1. **Asynchronous Batch Questioning**: Trello is asynchronous, not real-time. Instead of asking one question at a time, compile all relevant clarifying and requirement questions for this turn into a single structured list. Keep language simple, non-technical, and direct.\n"
            "2. **Manage PM/User Expectations**: Explicitly state to the PM/members that this is an iterative, multi-turn grilling process and that follow-up questions are expected and necessary depending on their answers. Educate them not to lock down specs or rush to implementation prematurely.\n"
            "3. **Non-Technical POV (UI/UX First)**: Frame all discussions, questions, and option proposals from the perspective of user experience (UI/UX), visual layout, and product behavior rather than backend or database-level engineering. Use simple, friendly, and non-technical language tailored for PMs, designers, and other non-engineering stakeholders. You may include minor technical limits only if directly relevant (e.g. 'we currently limit users to a maximum of 3 resumes').\n"
            "4. **Identify UI/UX Chain Reactions**: Think holistically about the entire product journey. When a PM requests a feature or modification on a specific screen (e.g. the `/apply-with-ai` page), analyze how this change ripples across other parts of the system (e.g. the user's dashboard, settings, activity history, or billing). Explicitly point out these downstream UI/UX implications to the PMs so they can approve the full scope of the change.\n"
            "5. **Strict Gatekeeping (Do Not Skip to Spec)**: You MUST NOT transition to spec or implementation mode (and must not recommend moving the card to 'Ready for Spec') if there are still critical, unanswered questions—even if a PM or user explicitly tells you to go straight to spec or implementation. You must insist on getting answers or, at a minimum, an explicit acknowledgment from them that they have chosen to skip/bypass specific questions before you proceed.\n"
            "6. **Formulate Options & Ground in Code**: Before proposing options, perform code searches in the workspace directories. Propose three clear approaches:\n"
            "   - A quick/easy version (reusing existing components/logic to the maximum).\n"
            "   - An ideal version (perfectly engineered design).\n"
            "   - A compromise version (reasonable trade-off between speed and clean architecture).\n"
            "   - *Note on Branch Safety:* Before comparing code, run `git fetch origin` and check if your local branch differs from `origin/main`. Ground all architectural designs in the latest upstream `origin/main` code to avoid proposing changes based on stale or unstable feature branch code.\n"
            "7. **Adversarial Codex Review**: Before presenting options to the PM/members on Trello, you must get an adversarial second opinion/review on your proposed approaches from Codex:\n"
            "   - Call the `call_mcp_tool` tool with parameters: `ServerName: \"codex-mcp\"`, `ToolName: \"codex\"`, and `Arguments: {\"model\": \"gpt-5.5\", \"config\": {\"model_reasoning_effort\": \"high\"}, \"prompt\": \"Please review these three proposed UI/UX approaches for the Trello card and provide an adversarial second opinion, identifying hidden complexities, UX edge cases, and which approach/compromise makes the most sense. Propose any refinements. Approaches: [Insert your proposed approaches]\"}`.\n"
            "   - Refine and adjust your options/approaches based on Codex's feedback before presenting them.\n"
            "8. **Post Comments & Tag**: Post your final refined response as a comment on the Trello card. You MUST address the user who triggered/commented on the card (e.g. Natalie Luo / @natalieqq or other stakeholders). Do NOT tag @fobtastic (which is the account you are posting from). Keep sentences short and use bullet points for readability. Sign with \"- Love {AGENT_SIGNATURE_NAME}\".\n\n"
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
            "You are reviewing and responding to Trello cards as the Senior Software Architect on behalf of @fobtastic (Chris Tou). "
            "Use the Trello API to interact with the specific card that triggered you (Trello credentials are available in the environment variables: "
            "TRELLO_API_KEY, TRELLO_SECRET, and TRELLO_API_TOKEN).\n\n"
            "This is a General Discussion trigger. The comment is conversational, seeking general feedback/ideas or asking general questions:\n"
            "1. Respond constructively and collaboratively as appropriate.\n"
            "2. Keep language simple, non-technical, and scannable.\n"
            "3. **Non-Technical POV (UI/UX First)**: Frame your answers around user experience and interface presentation. Speak in user-facing terms rather than backend system behaviors, keeping the non-engineering audience (PMs, designers) in mind.\n"
            f"4. Post your response as a comment on the Trello card, addressing the stakeholders who initiated the discussion (do NOT tag @fobtastic since you are acting on behalf of this account). Sign with \"- Love {AGENT_SIGNATURE_NAME}\".\n\n"
            "### Trello Helper Utility (MANDATORY)\n"
            "You MUST use the pre-installed CLI utility via run_command for ALL Trello operations (comment, move, add-label, remove-label). Do NOT construct raw `curl` commands, do NOT use inline HTTP request scripts, and do NOT write custom python files for Trello API calls. You must invoke the helper exactly as follows:\n"
            f"- Comment: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py comment <card_id> \"<text>\"`\n"
            f"- Move to List: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py move <card_id> \"<list_name>\"`\n"
            f"- Add Label: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py add-label <card_id> \"<label_name>\"`\n"
            f"- Remove Label: `python3 /home/ubuntu/projects/agy-trello/.agents/plugins/trello-integration/sidecars/trello-webhook-receiver/trello_helper.py remove-label <card_id> \"<label_name>\"`\n"
        )

    # Load existing session map
    session_map = load_session_mapping()
    conversation_id = session_map.get(card_key)
    
    # Fallback to short ID from cardLink if not found
    card_link = payload.get("cardLink")
    if not conversation_id and card_link:
        short_id = extract_short_id(card_link)
        if short_id and short_id != card_key:
            conversation_id = session_map.get(short_id)
            if conversation_id:
                logging.info(f"[Trello Sidecar] Found session via short ID fallback '{short_id}': {conversation_id}")
                # Associate the new long ID/card_key with the existing conversation
                session_map[card_key] = conversation_id
                save_session_mapping(session_map)
    
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
    if workspaces_info:
        prompt += (
            f"--- Active Workspaces ---\n"
            f"{workspaces_info}\n"
            f"-------------------------\n\n"
        )
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
    
    logging.info(f"[Trello Sidecar] Running agy command: {' '.join(cmd)}")
    
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
                    session_map[card_key] = new_id
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

def run_agent_job(card_key, card_name_str, payload):
    logging.info(f"[Trello Sidecar] Worker thread starting execution for card: '{card_name_str}'")
    try:
        asyncio.run(trigger_agent(card_key, card_name_str, payload))
    except Exception as e:
        logging.error(f"[Trello Sidecar] Exception during concurrent agent execution: {e}")

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

# Start background worker thread
threading.Thread(target=queue_worker, daemon=True).start()

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
            if (not card_id or not str(card_id).strip()) and "action" in payload:
                card_data = payload.get("action", {}).get("data", {}).get("card", {})
                card_id = card_data.get("id")
                card_name = card_data.get("name", "Unnamed Card")
                card_desc = card_data.get("desc", "")
                card_link = card_data.get("shortUrl", "")
                list_name = payload.get("action", {}).get("data", {}).get("list", {}).get("name", "")
                
                action_data = payload.get("action", {})
                if action_data.get("type") == "commentCard":
                    comment = action_data.get("data", {}).get("text", "")
            
            # Ensure the resolved card ID is injected back into the payload so the agent can use it
            if card_id:
                payload["cardId"] = card_id
            
            card_key = card_id or card_name or "Unnamed Card"
            card_name_str = card_name or "Unnamed Card"
            
            # Enqueue the webhook payload for concurrent background execution
            logging.info(f"[Trello Sidecar] Enqueueing agent trigger for card: '{card_name_str}'")
            task_queue.put((card_key, card_name_str, payload))

            if card_id:
                # Post acknowledgement comment asynchronously to not block the webhook response
                ack_text = f"Got it. Let me look into this...\n\n- Love {AGENT_SIGNATURE_NAME}"
                threading.Thread(target=post_trello_comment, args=(card_id, ack_text), daemon=True).start()
            
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

    # Bind only to 127.0.0.1 for secure local-only listening when testing/funneling
    server_address = ('127.0.0.1', port)
    httpd = HTTPServer(server_address, WebhookHandler)
    logging.info(f"[Trello Sidecar] Server listening on 127.0.0.1:{port}...")
    
    # Start the Tailscale Funnel in the background
    start_tailscale_funnel(port)
    
    try:
        httpd.serve_forever()
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
