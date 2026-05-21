import os
import signal
import sys
import subprocess
import threading
from pathlib import Path

from flask import Flask

app = Flask(__name__)

hermes_home = Path.home() / ".hermes"
hermes_home.mkdir(parents=True, exist_ok=True)

config_src = Path(__file__).parent / "config.yaml"
config_dst = hermes_home / "config.yaml"
if config_src.exists() and not config_dst.exists():
    config_dst.write_text(config_src.read_text())

dotenv_path = hermes_home / ".env"
lines = ["GATEWAY_ALLOW_ALL_USERS=true"]
telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN")
if telegram_token:
    lines.append(f"TELEGRAM_BOT_TOKEN={telegram_token}")
for i in range(1, 7):
    key_name = f"NVIDIA_NIM_KEY_{i}"
    val = os.environ.get(key_name)
    if val:
        lines.append(f"{key_name}={val}")
dotenv_path.write_text("\n".join(lines) + "\n")

env = os.environ.copy()
env["HERMES_HOME"] = str(hermes_home)

for i in range(1, 7):
    key_name = f"NVIDIA_NIM_KEY_{i}"
    if key_name in env:
        env[key_name] = env[key_name]

if "NVIDIA_NIM_KEY_1" in env:
    env.setdefault("NVIDIA_API_KEY", env["NVIDIA_NIM_KEY_1"])

proc = subprocess.Popen(
    ["hermes", "gateway"],
    env=env,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
)


def log_output(proc):
    for line in proc.stdout:
        print(f"[HERMES] {line}", end="", flush=True)


threading.Thread(target=log_output, args=(proc,), daemon=True).start()


def shutdown(signum, frame):
    proc.terminate()
    proc.wait(timeout=10)
    sys.exit(0)


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


@app.route("/")
def index():
    return "Hermes Status: ONLINE", 200, {"Content-Type": "text/plain"}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
