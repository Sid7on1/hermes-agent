import os
import sys
import subprocess
import threading

from flask import Flask

app = Flask(__name__)

env = os.environ.copy()

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


@app.route("/")
def index():
    return "Hermes Status: ONLINE", 200, {"Content-Type": "text/plain"}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
