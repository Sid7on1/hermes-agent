import os
import signal
import sys
import subprocess
import threading
import time
import resource
from pathlib import Path
from io import BytesIO

import itertools
import requests
from flask import Flask, request, Response

app = Flask(__name__)

hermes_home = Path.home() / ".hermes"
hermes_home.mkdir(parents=True, exist_ok=True)

# --- Supabase Sync Setup ---
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase_client = None

if supabase_url and supabase_key:
    try:
        from supabase import create_client, Client
        supabase_client: Client = create_client(supabase_url, supabase_key)
        print("[FLASK] Supabase Storage: Connected", flush=True)
    except Exception as e:
        print(f"[FLASK] Supabase Storage: Connection failed ({e})", flush=True)

def download_from_supabase(path_str):
    if not supabase_client: return
    try:
        res = supabase_client.storage.from_("hermes-state").download(path_str)
        local_path = hermes_home / path_str
        local_path.write_bytes(res)
        print(f"[FLASK] Supabase Sync: Downloaded {path_str}", flush=True)
    except Exception as e:
        print(f"[FLASK] Supabase Sync: Download failed for {path_str} ({e})", flush=True)

def upload_to_supabase(path_str):
    if not supabase_client: return
    local_path = hermes_home / path_str
    if local_path.exists():
        try:
            supabase_client.storage.from_("hermes-state").upload(path_str, local_path.read_bytes(), {"upsert": "true"})
            print(f"[FLASK] Supabase Sync: Uploaded {path_str}", flush=True)
        except Exception as e:
            print(f"[FLASK] Supabase Sync: Upload failed for {path_str} ({e})", flush=True)

def sync_all_to_supabase():
    """Upload all state files to Supabase."""
    if not supabase_client:
        return
    upload_to_supabase("state.db")
    upload_to_supabase("config.yaml")
    upload_to_supabase(".env")

# Restore state from cloud
if supabase_client:
    download_from_supabase("state.db")
    download_from_supabase("config.yaml")
    download_from_supabase(".env")
# --------------------------

config_src = Path(__file__).parent / "config.yaml"
config_dst = hermes_home / "config.yaml"
if config_src.exists() and not config_dst.exists():
    config_dst.write_text(config_src.read_text())

dotenv_path = hermes_home / ".env"
lines = []
telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN")
if telegram_token:
    lines.append(f"TELEGRAM_BOT_TOKEN={telegram_token}")

nvidia_keys = []
for i in range(1, 7):
    key_name = f"NVIDIA_NIM_KEY_{i}"
    val = os.environ.get(key_name)
    if val:
        lines.append(f"{key_name}={val}")
        nvidia_keys.append(val)
honcho_key = os.environ.get("HONCHO_API_KEY")
if honcho_key:
    lines.append(f"HONCHO_API_KEY={honcho_key}")
    honcho_dir = Path.home() / ".honcho"
    honcho_dir.mkdir(parents=True, exist_ok=True)
    honcho_config = honcho_dir / "config.json"
    if not honcho_config.exists():
        import json
        honcho_config.write_text(json.dumps({"enabled": True}))

dotenv_path.write_text("\n".join(lines) + "\n")

# --- Thread-safe key rotation with per-key rate tracking ---
key_lock = threading.Lock()
key_index = 0
key_last_used = {}        # key_index -> last_used_timestamp
key_fail_count = {}       # key_index -> consecutive 403 count
key_blacklisted = set()   # set of blacklisted key indices
KEY_COOLDOWN_SECS = 1.5   # minimum seconds between uses of the same key
KEY_BLACKLIST_THRESHOLD = 3  # 403s before blacklisting

def get_next_key():
    """Get the next available key, respecting cooldowns and blacklist.
    Returns (key_value, key_idx) or (None, None) if all keys exhausted."""
    global key_index
    with key_lock:
        if not nvidia_keys:
            return None, None
        now = time.time()
        n = len(nvidia_keys)
        # Try up to n keys to find one that's not blacklisted and not on cooldown
        for _ in range(n):
            idx = key_index % n
            key_index += 1
            if idx in key_blacklisted:
                continue
            last = key_last_used.get(idx, 0)
            if now - last < KEY_COOLDOWN_SECS:
                continue
            key_last_used[idx] = now
            return nvidia_keys[idx], idx
        # All keys on cooldown or blacklisted — return the least recently used non-blacklisted key
        available = [(key_last_used.get(i, 0), i) for i in range(n) if i not in key_blacklisted]
        if not available:
            return None, None
        available.sort()
        idx = available[0][1]
        key_last_used[idx] = now
        return nvidia_keys[idx], idx

def report_key_success(idx):
    """Reset failure counter on successful use."""
    with key_lock:
        key_fail_count[idx] = 0

def report_key_failure(idx, status_code):
    """Track 403 failures and blacklist after threshold."""
    if status_code != 403:
        return
    with key_lock:
        key_fail_count[idx] = key_fail_count.get(idx, 0) + 1
        count = key_fail_count[idx]
        if count >= KEY_BLACKLIST_THRESHOLD:
            key_blacklisted.add(idx)
            active = len(nvidia_keys) - len(key_blacklisted)
            print(f"[FLASK-PROXY] ⛔ Key #{idx+1} blacklisted after {count} consecutive 403s. "
                  f"Active keys: {active}/{len(nvidia_keys)}", flush=True)
        else:
            print(f"[FLASK-PROXY] ⚠️ Key #{idx+1} rejected (403) — failure {count}/{KEY_BLACKLIST_THRESHOLD}",
                  flush=True)

# --- Probe request filtering ---
# Hermes probes these endpoints to detect backend type; they never exist on NVIDIA.
# Returning 404 immediately avoids consuming a key rotation.
PROBE_PATHS = frozenset({"props", "version"})
PROBE_PREFIXES = ("api/",)

def is_probe_request(path):
    """Returns True if this is a Hermes backend-detection probe that should be short-circuited."""
    if path in PROBE_PATHS:
        return True
    for prefix in PROBE_PREFIXES:
        if path.startswith(prefix):
            return True
    return False

# --------------------------

port = int(os.environ.get("PORT", 8080))
local_proxy_url = f"http://127.0.0.1:{port}/v1"

env = os.environ.copy()
env["HERMES_HOME"] = str(hermes_home)
env["PYTHONUNBUFFERED"] = "1"

for i in range(1, 7):
    key_name = f"NVIDIA_NIM_KEY_{i}"
    if key_name in env:
        env[key_name] = env[key_name]

if "NVIDIA_NIM_KEY_1" in env:
    env.setdefault("NVIDIA_API_KEY", env["NVIDIA_NIM_KEY_1"])

env["NVIDIA_BASE_URL"] = local_proxy_url
env["NVIDIA_API_BASE"] = local_proxy_url
env["OPENAI_BASE_URL"] = local_proxy_url

if os.environ.get("DATABASE_URL"):
    print("[FLASK] Supabase Memory: Enabled", flush=True)
else:
    print("[FLASK] Supabase Memory: Not configured (add DATABASE_URL to Render)", flush=True)

print(f"[FLASK] NVIDIA keys loaded: {len(nvidia_keys)}", flush=True)

hermes_proc = None

def run_hermes():
    global hermes_proc
    print("[FLASK] Waiting 45 seconds to avoid Telegram polling conflict from old container...", flush=True)
    time.sleep(45)
    print("[FLASK] Starting Hermes Gateway...", flush=True)
    hermes_proc = subprocess.Popen(
        ["hermes", "gateway"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    for line in hermes_proc.stdout:
        print(f"[HERMES] {line}", end="", flush=True)
    rc = hermes_proc.returncode
    print(f"[FLASK] Hermes Gateway exited with code {rc}", flush=True)


threading.Thread(target=run_hermes, daemon=True).start()


# --- Periodic state sync (every 10 min) ---
# This ensures state.db is saved even if the process is OOM-killed (SIGKILL skips shutdown handler)
def periodic_sync():
    while True:
        time.sleep(600)  # 10 minutes
        print("[FLASK] Periodic sync: saving state to Supabase...", flush=True)
        try:
            sync_all_to_supabase()
            print("[FLASK] Periodic sync: complete", flush=True)
        except Exception as e:
            print(f"[FLASK] Periodic sync: failed ({e})", flush=True)

if supabase_client:
    threading.Thread(target=periodic_sync, daemon=True).start()
    print("[FLASK] Periodic sync: enabled (every 10 min)", flush=True)


# --- Memory monitoring (every 60s) ---
MEMORY_WARNING_MB = 400   # trigger early sync
MEMORY_LIMIT_MB = 512     # Render free tier limit

def get_rss_mb():
    """Get current RSS memory usage in MB."""
    try:
        rss_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # macOS returns bytes, Linux returns KB
        if sys.platform == "darwin":
            return rss_bytes / (1024 * 1024)
        else:
            return rss_bytes / 1024
    except Exception:
        return 0

def memory_monitor():
    warned = False
    while True:
        time.sleep(60)
        rss = get_rss_mb()
        active_keys = len(nvidia_keys) - len(key_blacklisted)
        print(f"[FLASK] Memory: {rss:.0f} MB / {MEMORY_LIMIT_MB} MB | "
              f"Keys: {active_keys}/{len(nvidia_keys)} active", flush=True)
        if rss > MEMORY_WARNING_MB and not warned:
            warned = True
            print(f"[FLASK] ⚠️ Memory above {MEMORY_WARNING_MB} MB — triggering early Supabase sync", flush=True)
            try:
                sync_all_to_supabase()
            except Exception as e:
                print(f"[FLASK] Early sync failed: {e}", flush=True)
        elif rss <= MEMORY_WARNING_MB:
            warned = False

threading.Thread(target=memory_monitor, daemon=True).start()


def shutdown(signum, frame):
    print("[FLASK] Received shutdown signal...", flush=True)
    if hermes_proc is not None:
        hermes_proc.terminate()
        try:
            hermes_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            hermes_proc.kill()
    
    # Save state to cloud
    if supabase_client:
        print("[FLASK] Saving memory to Supabase...", flush=True)
        sync_all_to_supabase()
    
    sys.exit(0)


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


@app.route("/")
def index():
    return "Hermes Status: ONLINE", 200, {"Content-Type": "text/plain"}

@app.route("/health")
def health():
    rss = get_rss_mb()
    active_keys = len(nvidia_keys) - len(key_blacklisted)
    status = {
        "status": "online",
        "memory_mb": round(rss, 1),
        "memory_limit_mb": MEMORY_LIMIT_MB,
        "nvidia_keys_active": active_keys,
        "nvidia_keys_total": len(nvidia_keys),
        "nvidia_keys_blacklisted": list(key_blacklisted),
        "hermes_running": hermes_proc is not None and hermes_proc.poll() is None,
    }
    import json
    return json.dumps(status, indent=2), 200, {"Content-Type": "application/json"}

@app.route("/v1/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
def proxy(path):
    # Short-circuit probe requests — don't waste key rotations
    if is_probe_request(path):
        return "Not Found", 404

    if not nvidia_keys:
        return "No NVIDIA keys configured", 500
    
    target_url = f"https://integrate.api.nvidia.com/v1/{path}"
    
    headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'authorization']}
    
    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    
    max_attempts = len(nvidia_keys) - len(key_blacklisted)
    if max_attempts <= 0:
        return "All NVIDIA keys blacklisted (403). Check your keys in Render env vars.", 503
    
    for attempt in range(max_attempts):
        next_key, idx = get_next_key()
        if next_key is None:
            break
        
        headers["Authorization"] = f"Bearer {next_key}"
        print(f"[FLASK-PROXY] Attempt {attempt+1}/{max_attempts} to {target_url} (key #{idx+1})", flush=True)
        
        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=request.get_data(),
            cookies=request.cookies,
            allow_redirects=False,
            stream=True
        )
        
        if resp.status_code == 403:
            report_key_failure(idx, 403)
            continue
        elif resp.status_code == 401:
            report_key_failure(idx, 401)
            print(f"[FLASK-PROXY] Key #{idx+1} unauthorized (401)", flush=True)
            continue
        
        # Success (or a non-auth error like 429 that Hermes handles with its own retry)
        report_key_success(idx)
        resp_headers = [(name, value) for (name, value) in resp.raw.headers.items()
                   if name.lower() not in excluded_headers]
        return Response(resp.iter_content(chunk_size=1024), resp.status_code, resp_headers)
    
    return "All NVIDIA keys failed authentication", 503

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=port)
