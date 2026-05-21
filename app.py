import os
import signal
import sys
import subprocess
import threading
from pathlib import Path

import itertools
import requests
from flask import Flask, request, Response

app = Flask(__name__)

hermes_home = Path.home() / ".hermes"
hermes_home.mkdir(parents=True, exist_ok=True)

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
dotenv_path.write_text("\n".join(lines) + "\n")

key_cycle = itertools.cycle(nvidia_keys) if nvidia_keys else None

env = os.environ.copy()
env["HERMES_HOME"] = str(hermes_home)

for i in range(1, 7):
    key_name = f"NVIDIA_NIM_KEY_{i}"
    if key_name in env:
        env[key_name] = env[key_name]

if "NVIDIA_NIM_KEY_1" in env:
    env.setdefault("NVIDIA_API_KEY", env["NVIDIA_NIM_KEY_1"])

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
    )
    for line in hermes_proc.stdout:
        print(f"[HERMES] {line}", end="", flush=True)


threading.Thread(target=run_hermes, daemon=True).start()


def shutdown(signum, frame):
    print("[FLASK] Received shutdown signal...", flush=True)
    if hermes_proc is not None:
        hermes_proc.terminate()
        try:
            hermes_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            hermes_proc.kill()
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
    
    next_key = next(key_cycle)
    headers["Authorization"] = f"Bearer {next_key}"
    print(f"[FLASK-PROXY] Proxying request to {target_url} using round-robin key...", flush=True)
    
    resp = requests.request(
        method=request.method,
        url=target_url,
        headers=headers,
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False,
        stream=True
    )
    
    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    resp_headers = [(name, value) for (name, value) in resp.raw.headers.items()
               if name.lower() not in excluded_headers]
               
    return Response(resp.iter_content(chunk_size=1024), resp.status_code, resp_headers)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=port)
