import os
import signal
import sys
import subprocess
import threading
import time
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

key_cycle = itertools.cycle(nvidia_keys) if nvidia_keys else None

env = os.environ.copy()
env["HERMES_HOME"] = str(hermes_home)
env["PYTHONUNBUFFERED"] = "1"

for i in range(1, 7):
    key_name = f"NVIDIA_NIM_KEY_{i}"
    if key_name in env:
        env[key_name] = env[key_name]

if "NVIDIA_NIM_KEY_1" in env:
    env.setdefault("NVIDIA_API_KEY", env["NVIDIA_NIM_KEY_1"])

if os.environ.get("DATABASE_URL"):
    print("[FLASK] Supabase Memory: Enabled", flush=True)
else:
    print("[FLASK] Supabase Memory: Not configured (add DATABASE_URL to Render)", flush=True)

port = int(os.environ.get("PORT", 8080))
local_proxy_url = f"http://127.0.0.1:{port}/v1"
env["NVIDIA_BASE_URL"] = local_proxy_url
env["NVIDIA_API_BASE"] = local_proxy_url
env["OPENAI_BASE_URL"] = local_proxy_url

hermes_proc = None

def run_hermes():
    global hermes_proc
    print("[FLASK] Waiting 45 seconds to avoid Telegram polling conflict from old container...", flush=True)
    import time
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
        upload_to_supabase("state.db")
        upload_to_supabase("config.yaml")
        upload_to_supabase(".env")
    
    sys.exit(0)


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


@app.route("/")
def index():
    return "Hermes Status: ONLINE", 200, {"Content-Type": "text/plain"}

@app.route("/v1/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
def proxy(path):
    if not key_cycle:
        return "No NVIDIA keys configured", 500
    
    target_url = f"https://integrate.api.nvidia.com/v1/{path}"
    
    headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'authorization']}
    
    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    
    max_attempts = len(nvidia_keys)
    for attempt in range(max_attempts):
        next_key = next(key_cycle)
        headers["Authorization"] = f"Bearer {next_key}"
        print(f"[FLASK-PROXY] Attempt {attempt+1}/{max_attempts} to {target_url} with key...", flush=True)
        
        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=request.get_data(),
            cookies=request.cookies,
            allow_redirects=False,
            stream=True
        )
        
        if resp.status_code not in (401, 403):
            resp_headers = [(name, value) for (name, value) in resp.raw.headers.items()
                       if name.lower() not in excluded_headers]
            return Response(resp.iter_content(chunk_size=1024), resp.status_code, resp_headers)
        
        print(f"[FLASK-PROXY] Key rejected ({resp.status_code}), trying next...", flush=True)
    
    return "All NVIDIA keys failed authentication", 503

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=port)
