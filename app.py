import os
import subprocess

from flask import Flask

app = Flask(__name__)

subprocess.Popen(
    ["python", "-m", "hermes.gateway.telegram"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)


@app.route("/")
def index():
    return "Hermes Status: ONLINE", 200, {"Content-Type": "text/plain"}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
