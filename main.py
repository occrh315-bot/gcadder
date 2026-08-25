import os
import sys
import time
import json
import urllib.parse
import logging
import gc
import requests
from flask import Flask, request, render_template, Response

try:
    from instagrapi import Client
    from instagrapi.exceptions import LoginRequired, RateLimitError, ClientError
except ImportError:
    print("instagrapi not found. Run: pip install instagrapi")
    sys.exit(1)

# ─── CONFIG ──────────────────────────────────────────────
SESSION_ID = os.environ.get("SESSION_ID", "")
DEFAULT_DELAY = 20
MIN_DELAY = 10
API_TIMEOUT = 60
THREAD_SCAN_LIMIT = 500   # pagination handle karega

# ─── LOGGING ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── FLASK APP ────────────────────────────────────────────
app = Flask(__name__)

# ─── HELPERS ─────────────────────────────────────────────
def decode_session(session):
    if not session:
        return session
    try:
        return urllib.parse.unquote(session)
    except:
        return session

def sse_event(event_type, message):
    timestamp = time.strftime("%H:%M:%S")
    data = json.dumps({"type": event_type, "msg": message, "time": timestamp})
    return f"data: {data}\n\n"

def retry_api_call(func, *args, max_retries=2, **kwargs):
    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except (RateLimitError, ClientError, requests.Timeout, requests.ConnectionError) as e:
            if attempt == max_retries:
                raise
            logger.warning(f"⚠️ API call failed ({e}), retrying {attempt+1}/{max_retries}...")
            time.sleep(5 * (attempt + 1))
    return None

# ─── LOGIN ──────────────────────────────────────────────
def login_session(session_id):
    session_id = decode_session(session_id)
    try:
        cl = Client()
        cl.timeout = API_TIMEOUT
        cl.login_by_sessionid(session_id)
        return cl
    except Exception as e:
        logger.error(f"Login failed: {e}")
        return None

# ─── FETCH ALL GROUPS (PAGINATION) ──────────────────────
def fetch_all_groups(cl, limit=THREAD_SCAN_LIMIT):
    all_group_threads = []
    cursor = None
    while len(all_group_threads) < limit:
        try:
            data = {
                "visual_message_return_type": "unseen",
                "thread_message_limit": "1",
                "persistentBadging": "true",
                "limit": "20",
            }
            if cursor:
                data["cursor"] = cursor
            response = cl.private_request("direct_v2/inbox/", data=data)
            inbox = response.get("inbox", {})
            threads = inbox.get("threads", [])
            for t in threads:
                if t.get("is_group") or len(t.get("users", [])) > 1:
                    tid = str(t.get("thread_v2_id") or t.get("thread_id") or t.get("pk"))
                    if tid:
                        all_group_threads.append(tid)
            next_cursor = inbox.get("oldest_cursor") or inbox.get("next_cursor")
            has_older = inbox.get("has_older", False)
            if not has_older or not next_cursor:
                break
            cursor = next_cursor
            time.sleep(1)
        except Exception as e:
            logger.warning(f"Fetch threads failed: {e}")
            break
    return list(dict.fromkeys(all_group_threads))

# ─── ADD USER WITH FALLBACK METHODS ──────────────────────
def add_user_to_thread(cl, thread_id, user_id):
    if not thread_id or not user_id:
        return False

    # Method 1: direct_add_user
    try:
        if hasattr(cl, 'direct_add_user'):
            result = retry_api_call(cl.direct_add_user, thread_id, user_id)
            if result is not None:
                logger.info(f"✅ Added user {user_id} via direct_add_user")
                return True
    except Exception as e:
        logger.warning(f"⚠️ direct_add_user failed: {e}")

    # Method 2: private_request
    try:
        url = f"direct_v2/threads/{thread_id}/add_user/"
        data = {"user_id": str(user_id)}
        result = retry_api_call(cl.private_request, url, data)
        if result and result.get("status") == "ok":
            logger.info(f"✅ Added user {user_id} via private_request")
            return True
    except Exception as e:
        logger.warning(f"⚠️ private_request add failed: {e}")

    # Method 3: direct_messages.add_user
    try:
        if hasattr(cl, 'direct_messages') and hasattr(cl.direct_messages, 'add_user'):
            result = retry_api_call(cl.direct_messages.add_user, thread_id, user_id)
            if result is not None:
                logger.info(f"✅ Added user {user_id} via direct_messages.add_user")
                return True
    except Exception as e:
        logger.warning(f"⚠️ direct_messages.add_user failed: {e}")

    return False

# ─── MAIN ENGINE ────────────────────────────────────────
def add_users_stream(session_id, group_ids, usernames, delay_seconds, batch_size, batch_cooldown):
    yield sse_event("info", f"🚀 Starting Add Users (delay: {delay_seconds}s)...")
    yield sse_event("info", f"👥 Users to add: {', '.join(usernames)}")
    yield sse_event("info", f"🔢 Total groups selected: {len(group_ids)}")
    if batch_size > 0:
        yield sse_event("info", f"🛑 Batch cooldown: after every {batch_size} groups, wait {batch_cooldown}s")

    cl = login_session(session_id)
    if not cl:
        yield sse_event("error", "❌ Login failed.")
        return

    # Resolve user IDs
    user_ids = []
    for u in usernames:
        try:
            uid = retry_api_call(cl.user_id_from_username, u)
            user_ids.append(uid)
        except Exception as e:
            yield sse_event("error", f"❌ Could not resolve user {u}: {e}")
            return

    yield sse_event("success", f"✅ {len(user_ids)} users resolved")

    for idx, thread_id in enumerate(group_ids):
        if not thread_id:
            continue
        yield sse_event("info", f"🌼 Processing GC {idx+1}/{len(group_ids)} (thread: {thread_id[:10]}...)")

        # Add each user
        for i, uid in enumerate(user_ids):
            username = usernames[i]
            yield sse_event("info", f"👤 Adding {username}...")
            success = add_user_to_thread(cl, thread_id, uid)
            if success:
                yield sse_event("success", f"✅ Added {username}")
            else:
                yield sse_event("error", f"❌ Failed to add {username}")

        gc.collect()

        # Batch cooldown
        if batch_size > 0 and (idx + 1) % batch_size == 0 and idx + 1 < len(group_ids):
            yield sse_event("info", f"🛑 Batch cooldown: waiting {batch_cooldown}s...")
            time.sleep(batch_cooldown)

        # Normal delay
        if idx < len(group_ids) - 1:
            yield sse_event("info", f"⏳ Waiting {delay_seconds}s...")
            time.sleep(delay_seconds)

    yield sse_event("success", "🎉 All groups processed!")

# ─── ROUTES ──────────────────────────────────────────────
@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/api/fetch-groups", methods=["POST"])
def fetch_groups_api():
    data = request.json
    session_id = data.get("session_id", "").strip() or SESSION_ID
    if not session_id:
        return jsonify({"success": False, "error": "Session ID required"}), 400
    try:
        cl = login_session(session_id)
        if not cl:
            return jsonify({"success": False, "error": "Login failed"}), 400
        group_ids = fetch_all_groups(cl)
        group_details = []
        for tid in group_ids:
            try:
                thread = cl.direct_thread(int(tid))
                name = thread.thread_title or tid
            except:
                name = tid
            group_details.append({"id": tid, "name": name})
        return jsonify({"success": True, "groups": group_details})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

@app.route("/api/start-add", methods=["POST"])
def start_add():
    session_id = request.form.get("session_id", "").strip() or SESSION_ID
    if not session_id:
        return "❌ Session ID required.", 400

    group_ids = request.form.get("group_ids", "").strip().split(",") if request.form.get("group_ids") else []
    group_ids = [g.strip() for g in group_ids if g.strip()]
    if not group_ids:
        return "❌ No groups selected.", 400

    usernames_raw = request.form.get("usernames", "").strip()
    usernames = [u.strip() for u in usernames_raw.split(",") if u.strip()]
    if not usernames:
        return "❌ At least one username required.", 400

    try:
        delay = float(request.form.get("delay", DEFAULT_DELAY))
        if delay < MIN_DELAY:
            delay = MIN_DELAY
    except:
        delay = DEFAULT_DELAY

    try:
        batch_size = int(request.form.get("batch_size", 0))
    except:
        batch_size = 0
    try:
        batch_cooldown = int(request.form.get("batch_cooldown", 0))
    except:
        batch_cooldown = 0

    def generate():
        yield from add_users_stream(session_id, group_ids, usernames, delay, batch_size, batch_cooldown)

    return Response(generate(), mimetype="text/event-stream")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
