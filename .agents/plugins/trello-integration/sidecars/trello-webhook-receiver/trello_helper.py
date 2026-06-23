import sys
import os
import json
import urllib.request
import urllib.parse
import re

def load_zshrc_env():
    # Load credentials if not present in environment
    zshrc_path = os.path.expanduser("~/.zshrc")
    if os.path.exists(zshrc_path):
        try:
            with open(zshrc_path, "r") as f:
                content = f.read()
                for key in ["TRELLO_API_KEY", "TRELLO_SECRET", "TRELLO_API_TOKEN", "TRELLO_TOKEN"]:
                    if key not in os.environ:
                        match = re.search(fr'export {key}=(.*)', content)
                        if match:
                            os.environ[key] = match.group(1).strip().strip('"').strip("'")
        except Exception:
            pass

load_zshrc_env()

API_KEY = os.environ.get("TRELLO_API_KEY")
API_TOKEN = os.environ.get("TRELLO_API_TOKEN") or os.environ.get("TRELLO_TOKEN")

if not API_KEY or not API_TOKEN:
    print("Error: Missing TRELLO_API_KEY or TRELLO_API_TOKEN in environment / ~/.zshrc")
    sys.exit(1)

def make_request(url, method="GET", data=None):
    req = urllib.request.Request(url, method=method)
    if data is not None:
        encoded_data = urllib.parse.urlencode(data).encode("utf-8")
        req.data = encoded_data
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8")), None
    except Exception as e:
        return None, str(e)

def find_list_id(card_id, list_name):
    # Get card details to find the board ID
    url = f"https://api.trello.com/1/cards/{card_id}?key={API_KEY}&token={API_TOKEN}&fields=idBoard"
    card_data, err = make_request(url)
    if err or not card_data:
        return None, f"Failed to get card board ID: {err}"
    
    board_id = card_data.get("idBoard")
    if not board_id:
        return None, "Board ID not found on card"
        
    # Get lists on that board
    lists_url = f"https://api.trello.com/1/boards/{board_id}/lists?key={API_KEY}&token={API_TOKEN}&fields=name"
    lists, err = make_request(lists_url)
    if err or not lists:
        return None, f"Failed to get board lists: {err}"
        
    for lst in lists:
        if lst.get("name").strip().lower() == list_name.strip().lower():
            return lst.get("id"), None
            
    return None, f"List '{list_name}' not found on board"

def find_label_id(card_id, label_name):
    # Get card details to find board ID
    url = f"https://api.trello.com/1/cards/{card_id}?key={API_KEY}&token={API_TOKEN}&fields=idBoard"
    card_data, err = make_request(url)
    if err or not card_data:
        return None, f"Failed to get card board ID: {err}"
    
    board_id = card_data.get("idBoard")
    if not board_id:
        return None, "Board ID not found on card"
        
    # Get labels on board
    labels_url = f"https://api.trello.com/1/boards/{board_id}/labels?key={API_KEY}&token={API_TOKEN}&fields=name"
    labels, err = make_request(labels_url)
    if err or not labels:
        return None, f"Failed to get board labels: {err}"
        
    for lbl in labels:
        if lbl.get("name").strip().lower() == label_name.strip().lower():
            return lbl.get("id"), None
            
    return None, f"Label '{label_name}' not found on board"

def cmd_comment(card_id, text):
    url = f"https://api.trello.com/1/cards/{card_id}/actions/comments?key={API_KEY}&token={API_TOKEN}"
    res, err = make_request(url, method="POST", data={"text": text})
    if err:
        print(f"Error posting comment: {err}")
        sys.exit(1)
    print("Successfully posted comment.")

def cmd_move(card_id, list_name):
    list_id, err = find_list_id(card_id, list_name)
    if err:
        print(f"Error: {err}")
        sys.exit(1)
    url = f"https://api.trello.com/1/cards/{card_id}?key={API_KEY}&token={API_TOKEN}"
    res, err = make_request(url, method="PUT", data={"idList": list_id})
    if err:
        print(f"Error moving card: {err}")
        sys.exit(1)
    print(f"Successfully moved card to list '{list_name}'.")

def cmd_add_label(card_id, label_name):
    label_id, err = find_label_id(card_id, label_name)
    if err:
        # Try creating the label directly if not found
        url = f"https://api.trello.com/1/cards/{card_id}/labels?key={API_KEY}&token={API_TOKEN}"
        res, err2 = make_request(url, method="POST", data={"name": label_name, "color": "blue"})
        if err2:
            print(f"Error creating/adding label: {err2}")
            sys.exit(1)
        print(f"Successfully created and added label '{label_name}'.")
        return
        
    url = f"https://api.trello.com/1/cards/{card_id}/idLabels?key={API_KEY}&token={API_TOKEN}"
    res, err = make_request(url, method="POST", data={"value": label_id})
    if err:
        print(f"Error adding label ID: {err}")
        sys.exit(1)
    print(f"Successfully added label '{label_name}'.")

def cmd_remove_label(card_id, label_name):
    label_id, err = find_label_id(card_id, label_name)
    if err:
        print(f"Error: {err}")
        sys.exit(1)
    url = f"https://api.trello.com/1/cards/{card_id}/idLabels/{label_id}?key={API_KEY}&token={API_TOKEN}"
    # Empty data for DELETE request
    res, err = make_request(url, method="DELETE", data={})
    if err:
        print(f"Error removing label: {err}")
        sys.exit(1)
    print(f"Successfully removed label '{label_name}'.")

def print_usage():
    print("Trello Helper CLI Utility")
    print("Usage:")
    print("  python3 trello_helper.py comment <card_id> <text>")
    print("  python3 trello_helper.py move <card_id> <list_name>")
    print("  python3 trello_helper.py add-label <card_id> <label_name>")
    print("  python3 trello_helper.py remove-label <card_id> <label_name>")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print_usage()
        sys.exit(1)
        
    cmd = sys.argv[1].lower()
    card_id = sys.argv[2]
    arg = " ".join(sys.argv[3:])
    
    if cmd == "comment":
        cmd_comment(card_id, arg)
    elif cmd == "move":
        cmd_move(card_id, arg)
    elif cmd == "add-label":
        cmd_add_label(card_id, arg)
    elif cmd == "remove-label":
        cmd_remove_label(card_id, arg)
    else:
        print(f"Unknown command: {cmd}")
        print_usage()
        sys.exit(1)
