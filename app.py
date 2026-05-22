import os
import gc
import json
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests as http_requests
from flask import Flask, request, Response
import yaml

# ╔══════════════════════════════════════════════════════════════╗
# ║                      CONFIGURATION                          ║
# ╚══════════════════════════════════════════════════════════════╝

app = Flask(__name__)

HERMES_HOME = Path.home() / ".hermes"
HERMES_HOME.mkdir(parents=True, exist_ok=True)

# Supabase
SUPABASE_BUCKET = "hermes-state"

# Sync
SYNC_INTERVAL_SECS = 600         # periodic incremental sync every 10 min
MAX_SYNC_FILE_SIZE_MB = 50       # skip files larger than this
# Exclude transient/regenerable dirs — everything else is synced
SYNC_EXCLUDE_DIRS = frozenset({"__pycache__", ".git", "cache", "sandbox", "tmp"})
SYNC_EXCLUDE_SUFFIXES = frozenset({".pyc", ".pyo", "-wal", "-shm", ".log", ".tmp"})

# Memory
MEMORY_CHECK_INTERVAL = 30       # seconds
MEMORY_WARNING_MB = 6000         # early sync trigger
MEMORY_CRITICAL_MB = 8000        # GC + sync
MEMORY_LIMIT_MB = 16384          # Hugging Face Spaces free tier

# Key rotation
KEY_COOLDOWN_BASE = 2.0          # seconds — base cooldown after 429
KEY_COOLDOWN_MAX = 30.0          # seconds — max cooldown per key
KEY_REQUEST_TIMEOUT = (10, 120)  # (connect, read) — generous read for streaming

# Hermes watchdog
HERMES_STARTUP_DELAY = 45        # seconds — let old Telegram poll lock expire
HERMES_MAX_RESTARTS = 5          # within HERMES_RESTART_WINDOW
HERMES_RESTART_WINDOW = 600      # seconds (10 min)
HERMES_RESTART_BASE_DELAY = 5    # seconds — base delay, doubles each restart

# Boot timestamp
BOOT_TIME = time.time()


# ╔══════════════════════════════════════════════════════════════╗
# ║                  SUPABASE SYNC ENGINE                        ║
# ║                                                              ║
# ║  Syncs the ENTIRE ~/.hermes/ directory:                      ║
# ║  state.db, skills/, memories/, SOUL.md, config.yaml,         ║
# ║  auth.json, cron/, sessions/ — everything the agent creates  ║
# ║                                                              ║
# ║  Uses a manifest (_manifest.json) for incremental uploads:   ║
# ║  only files whose mtime or size changed get re-uploaded.     ║
# ╚══════════════════════════════════════════════════════════════╝

supabase_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
supabase_key = os.environ.get("SUPABASE_KEY")  # service_role key
supabase_ready = bool(supabase_url and supabase_key)

# Direct REST headers — no SDK needed, saves ~50 MB RAM
_sb_headers = (
    {"Authorization": f"Bearer {supabase_key}", "apikey": supabase_key}
    if supabase_ready
    else {}
)

sync_lock = threading.Lock()
sync_manifest = {}  # rel_path -> {size, mtime, synced_at}
sync_stats = {
    "uploads": 0,
    "downloads": 0,
    "last_sync": None,
    "errors": 0,
    "files_synced": [],
}

if supabase_ready:
    # Verify connection with a quick HEAD request to the bucket
    try:
        r = http_requests.get(
            f"{supabase_url}/storage/v1/bucket/{SUPABASE_BUCKET}",
            headers=_sb_headers,
            timeout=10,
        )
        if r.status_code == 200:
            print("[SYNC] ✅ Supabase Storage connected (direct REST)", flush=True)
        else:
            print(f"[SYNC] ⚠️ Supabase bucket check: HTTP {r.status_code}", flush=True)
    except Exception as e:
        print(f"[SYNC] ❌ Supabase connection failed: {e}", flush=True)
        supabase_ready = False


def _should_sync(rel_path: str) -> bool:
    """Filter: skip transient files, keep everything else."""
    p = Path(rel_path)
    if any(part in SYNC_EXCLUDE_DIRS for part in p.parts):
        return False
    if p.suffix in SYNC_EXCLUDE_SUFFIXES:
        return False
    if p.name.endswith("-wal") or p.name.endswith("-shm"):
        return False
    return True


def _get_local_files() -> dict:
    """Walk ~/.hermes/ and return all syncable files with metadata."""
    files = {}
    for fpath in HERMES_HOME.rglob("*"):
        if not fpath.is_file():
            continue
        rel = str(fpath.relative_to(HERMES_HOME))
        if not _should_sync(rel):
            continue
        stat = fpath.stat()
        if stat.st_size > MAX_SYNC_FILE_SIZE_MB * 1024 * 1024:
            continue
        files[rel] = {"size": stat.st_size, "mtime": stat.st_mtime}
    return files


def _checkpoint_sqlite():
    """Flush SQLite WAL so state.db is self-contained before upload."""
    db_path = HERMES_HOME / "state.db"
    if not db_path.exists():
        return
    try:
        conn = sqlite3.connect(str(db_path), timeout=5)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    except Exception as e:
        print(f"[SYNC] SQLite checkpoint warning: {e}", flush=True)


def _download_one(rel_path: str) -> bool:
    """Download a single file from Supabase Storage to ~/.hermes/."""
    if not supabase_ready:
        return False
    try:
        url = f"{supabase_url}/storage/v1/object/{SUPABASE_BUCKET}/{rel_path}"
        r = http_requests.get(url, headers=_sb_headers, timeout=30)
        if r.status_code == 200:
            local = HERMES_HOME / rel_path
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(r.content)
            sync_stats["downloads"] += 1
            return True
        elif r.status_code == 404:
            return False  # expected on first run
        else:
            print(f"[SYNC] Download failed: {rel_path} — HTTP {r.status_code}", flush=True)
            sync_stats["errors"] += 1
            return False
    except Exception as e:
        print(f"[SYNC] Download failed: {rel_path} — {e}", flush=True)
        sync_stats["errors"] += 1
        return False


def _upload_one(rel_path: str) -> bool:
    """Upload a single file from ~/.hermes/ to Supabase Storage (upsert)."""
    if not supabase_ready:
        return False
    local = HERMES_HOME / rel_path
    if not local.exists():
        return False
    try:
        file_data = local.read_bytes()
        # Guess content type
        ct = "application/octet-stream"
        if rel_path.endswith((".json", ".yaml", ".yml", ".md", ".txt", ".env")):
            ct = "text/plain"

        url = f"{supabase_url}/storage/v1/object/{SUPABASE_BUCKET}/{rel_path}"
        headers = {**_sb_headers, "Content-Type": ct, "x-upsert": "true"}
        r = http_requests.post(url, headers=headers, data=file_data, timeout=30)
        if r.status_code in (200, 201):
            sync_stats["uploads"] += 1
            return True
        else:
            print(f"[SYNC] Upload failed: {rel_path} — HTTP {r.status_code}: {r.text[:200]}", flush=True)
            sync_stats["errors"] += 1
            return False
    except Exception as e:
        print(f"[SYNC] Upload failed: {rel_path} — {e}", flush=True)
        sync_stats["errors"] += 1
        return False


def _load_manifest() -> dict:
    """Download the sync manifest from Supabase."""
    if not supabase_ready:
        return {}
    try:
        url = f"{supabase_url}/storage/v1/object/{SUPABASE_BUCKET}/_manifest.json"
        r = http_requests.get(url, headers=_sb_headers, timeout=15)
        if r.status_code == 200:
            return r.json()
        return {}
    except Exception:
        return {}


def _save_manifest():
    """Upload the sync manifest to Supabase."""
    if not supabase_ready:
        return
    try:
        data = json.dumps(sync_manifest, indent=2, default=str).encode("utf-8")
        url = f"{supabase_url}/storage/v1/object/{SUPABASE_BUCKET}/_manifest.json"
        headers = {**_sb_headers, "Content-Type": "application/json", "x-upsert": "true"}
        r = http_requests.post(url, headers=headers, data=data, timeout=15)
        if r.status_code not in (200, 201):
            print(f"[SYNC] Manifest save failed: HTTP {r.status_code}", flush=True)
    except Exception as e:
        print(f"[SYNC] Manifest save failed: {e}", flush=True)


def sync_download_all():
    """Restore full state from Supabase on startup.

    Downloads everything: state.db (conversations, memory, FTS indexes),
    skills/ (learned + installed), memories/ (MEMORY.md, USER.md),
    SOUL.md, auth.json, cron/, config.yaml, .env, and anything else
    the agent previously created.
    """
    global sync_manifest
    if not supabase_ready:
        return

    print("[SYNC] 📥 Restoring state from Supabase...", flush=True)
    sync_manifest = _load_manifest()

    if not sync_manifest:
        print("[SYNC] No manifest found — first deployment or empty bucket", flush=True)
        # Try legacy download of the 3 original files
        for f in ("state.db", "config.yaml", ".env"):
            if _download_one(f):
                print(f"[SYNC]   ↳ legacy restore: {f}", flush=True)
        return

    ok, skip = 0, 0
    categories = {}  # group by top-level dir for logging
    for rel_path in sorted(sync_manifest.keys()):
        if rel_path == "_manifest.json":
            continue
        if _download_one(rel_path):
            ok += 1
            cat = rel_path.split("/")[0] if "/" in rel_path else "(root)"
            categories[cat] = categories.get(cat, 0) + 1
        else:
            skip += 1

    detail = ", ".join(f"{cat}: {n}" for cat, n in sorted(categories.items()))
    print(f"[SYNC] 📥 Restored {ok} files ({skip} skipped)", flush=True)
    if detail:
        print(f"[SYNC]   ↳ {detail}", flush=True)


def sync_upload_changed():
    """Incremental sync — only upload files that changed since last sync.

    Compares local mtimes/sizes against the manifest. Only changed files
    are uploaded, minimizing Supabase bandwidth usage.
    """
    global sync_manifest
    with sync_lock:
        if not supabase_ready:
            return

        _checkpoint_sqlite()
        local_files = _get_local_files()
        uploaded = []

        for rel_path, info in local_files.items():
            prev = sync_manifest.get(rel_path, {})
            if prev.get("mtime") == info["mtime"] and prev.get("size") == info["size"]:
                continue  # unchanged
            if _upload_one(rel_path):
                sync_manifest[rel_path] = {
                    "size": info["size"],
                    "mtime": info["mtime"],
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }
                uploaded.append(rel_path)

        if uploaded:
            _save_manifest()
            sync_stats["last_sync"] = datetime.now(timezone.utc).isoformat()
            sync_stats["files_synced"] = uploaded[-10:]  # keep last 10
            print(f"[SYNC] 📤 Synced {len(uploaded)} files: {', '.join(uploaded)}", flush=True)


def sync_upload_all():
    """Force-upload everything (used on shutdown)."""
    global sync_manifest
    with sync_lock:
        if not supabase_ready:
            return

        _checkpoint_sqlite()
        local_files = _get_local_files()
        uploaded = 0

        for rel_path, info in local_files.items():
            if _upload_one(rel_path):
                sync_manifest[rel_path] = {
                    "size": info["size"],
                    "mtime": info["mtime"],
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }
                uploaded += 1

        _save_manifest()
        sync_stats["last_sync"] = datetime.now(timezone.utc).isoformat()
        print(f"[SYNC] 📤 Full sync: {uploaded} files uploaded", flush=True)


# ╔══════════════════════════════════════════════════════════════╗
# ║                   NVIDIA KEY MANAGER                         ║
# ║                                                              ║
# ║  Adaptive rotation — no permanent blacklisting.              ║
# ║  Per-key exponential cooldown on 429; transient retry on 403 ║
# ║  Thread-safe with Lock.                                      ║
# ╚══════════════════════════════════════════════════════════════╝

nvidia_keys = []
for i in range(1, 7):
    val = os.environ.get(f"NVIDIA_NIM_KEY_{i}")
    if val and val not in nvidia_keys:
        nvidia_keys.append(val)

# Fallback to standard NVIDIA_API_KEY environment variable
api_key_val = os.environ.get("NVIDIA_API_KEY")
if api_key_val and api_key_val not in nvidia_keys:
    nvidia_keys.append(api_key_val)

key_lock = threading.Lock()
key_index = 0
key_state = {}  # idx -> {cooldown_until, consecutive_429, consecutive_403, total_ok, total_fail, last_used}

for i in range(len(nvidia_keys)):
    key_state[i] = {
        "cooldown_until": 0,
        "consecutive_429": 0,
        "consecutive_403": 0,
        "total_ok": 0,
        "total_fail": 0,
        "last_used": 0,
    }

# Hermes probes these to detect backend type — they never exist on NVIDIA
PROBE_PATHS = frozenset({"props", "version"})


def _is_probe(path: str) -> bool:
    """Return True for backend-detection probes that should be short-circuited."""
    return path in PROBE_PATHS or path.startswith("api/")


def get_next_key(exclude_indices=None):
    """Thread-safe key selection with adaptive cooldown.

    Returns (key_value, key_index) or (None, None) if all exhausted.
    Never permanently disables a key — uses time-based cooldowns only.
    """
    global key_index
    if exclude_indices is None:
        exclude_indices = set()

    with key_lock:
        if not nvidia_keys:
            return None, None
        n = len(nvidia_keys)
        now = time.time()

        eligible_indices = [i for i in range(n) if i not in exclude_indices]
        if not eligible_indices:
            return None, None

        # Pass 1: find an eligible key that's off cooldown
        for _ in range(n):
            idx = key_index % n
            key_index += 1
            if idx in exclude_indices:
                continue
            if now >= key_state[idx]["cooldown_until"]:
                key_state[idx]["last_used"] = now
                return nvidia_keys[idx], idx

        # Pass 2: all eligible keys on cooldown — pick the one that's soonest available
        soonest_idx = min(eligible_indices, key=lambda i: key_state[i]["cooldown_until"])
        key_state[soonest_idx]["last_used"] = now
        return nvidia_keys[soonest_idx], soonest_idx


def report_key_result(idx: int, status_code: int, headers=None):
    """Update per-key state based on API response."""
    with key_lock:
        s = key_state[idx]
        now = time.time()

        if 200 <= status_code < 300:
            s["total_ok"] += 1
            s["consecutive_429"] = 0
            s["consecutive_403"] = 0
            s["cooldown_until"] = 0

        elif status_code == 429:
            s["consecutive_429"] += 1
            s["total_fail"] += 1

            cooldown = None
            if headers:
                retry_after = headers.get("retry-after") or headers.get("Retry-After")
                if retry_after:
                    try:
                        cooldown = float(retry_after)
                    except ValueError:
                        pass
            
            if cooldown is None:
                # Exponential cooldown: 2, 4, 8, 16... capped
                cooldown = min(
                    KEY_COOLDOWN_BASE * (2 ** (s["consecutive_429"] - 1)),
                    KEY_COOLDOWN_MAX,
                )
            
            s["cooldown_until"] = now + cooldown
            print(
                f"[PROXY] Key #{idx+1} rate-limited (429) — "
                f"cooldown {cooldown:.0f}s (streak {s['consecutive_429']})",
                flush=True,
            )

        elif status_code == 403:
            s["consecutive_403"] += 1
            s["total_fail"] += 1
            # Short cooldown — transient, not permanent
            s["cooldown_until"] = now + 2.0
            print(
                f"[PROXY] Key #{idx+1} rejected (403) — "
                f"cooldown 2s (streak {s['consecutive_403']})",
                flush=True,
            )

        elif status_code == 401:
            s["total_fail"] += 1
            s["cooldown_until"] = now + 5.0
            print(f"[PROXY] Key #{idx+1} unauthorized (401) — cooldown 5s", flush=True)

        elif status_code >= 500:
            s["total_fail"] += 1
            s["cooldown_until"] = now + 3.0
            print(f"[PROXY] Key #{idx+1} server error ({status_code}) — cooldown 3s", flush=True)


# ╔══════════════════════════════════════════════════════════════╗
# ║                HERMES PROCESS MANAGER                        ║
# ║                                                              ║
# ║  Watchdog with auto-restart, exponential backoff,            ║
# ║  process-group killing, and crash-loop detection.            ║
# ╚══════════════════════════════════════════════════════════════╝

hermes_proc = None
hermes_lock = threading.Lock()
hermes_restarts = []  # timestamps of recent restarts
hermes_status = {
    "state": "initializing",
    "pid": None,
    "started_at": None,
    "restarts": 0,
    "last_exit_code": None,
}


def _new_process_group():
    """Create a new process group so we can kill all children on shutdown."""
    try:
        os.setsid()
    except OSError:
        pass


def _start_hermes(env_dict):
    """Start the Hermes gateway subprocess."""
    global hermes_proc
    hermes_proc = subprocess.Popen(
        ["hermes", "gateway"],
        env=env_dict,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        preexec_fn=_new_process_group,
    )
    hermes_status["pid"] = hermes_proc.pid
    hermes_status["started_at"] = datetime.now(timezone.utc).isoformat()
    hermes_status["state"] = "running"
    print(f"[WATCHDOG] 🚀 Hermes started (PID {hermes_proc.pid})", flush=True)


def _kill_hermes():
    """Kill the Hermes process and its entire process group."""
    if hermes_proc is None or hermes_proc.poll() is not None:
        return
    try:
        pgid = os.getpgid(hermes_proc.pid)
        os.killpg(pgid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        hermes_proc.terminate()
    try:
        hermes_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            pgid = os.getpgid(hermes_proc.pid)
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            hermes_proc.kill()


def run_hermes_watchdog(env_dict):
    """Main watchdog loop — starts Hermes and auto-restarts on crash."""
    global hermes_proc

    print(
        f"[WATCHDOG] Waiting {HERMES_STARTUP_DELAY}s for "
        f"Telegram polling lock to expire...",
        flush=True,
    )
    hermes_status["state"] = "waiting"
    time.sleep(HERMES_STARTUP_DELAY)

    while True:
        # Check restart budget
        now = time.time()
        hermes_restarts[:] = [
            t for t in hermes_restarts if now - t < HERMES_RESTART_WINDOW
        ]

        if len(hermes_restarts) >= HERMES_MAX_RESTARTS:
            hermes_status["state"] = "crash_loop"
            print(
                f"[WATCHDOG] ❌ Crash loop detected "
                f"({HERMES_MAX_RESTARTS} restarts in {HERMES_RESTART_WINDOW}s). "
                f"Hermes will NOT be restarted. Check logs.",
                flush=True,
            )
            sync_upload_changed()
            return

        with hermes_lock:
            _start_hermes(env_dict)

        # Stream output (blocks until process exits)
        if hermes_proc and hermes_proc.stdout:
            for line in hermes_proc.stdout:
                print(f"[HERMES] {line}", end="", flush=True)

        rc = hermes_proc.wait() if hermes_proc else -1
        hermes_status["state"] = "exited"
        hermes_status["last_exit_code"] = rc

        if rc == 0:
            print("[WATCHDOG] Hermes exited cleanly (code 0)", flush=True)
            break

        hermes_restarts.append(time.time())
        hermes_status["restarts"] += 1
        restart_num = len(hermes_restarts)
        delay = min(HERMES_RESTART_BASE_DELAY * (2 ** (restart_num - 1)), 60)

        exit_reason = "OOM killed" if rc == 137 else f"code {rc}"
        print(
            f"[WATCHDOG] ⚠️ Hermes crashed ({exit_reason}). "
            f"Restart {restart_num}/{HERMES_MAX_RESTARTS} in {delay}s...",
            flush=True,
        )

        # Save state before restart (in case next crash is an OOM SIGKILL)
        sync_upload_changed()
        time.sleep(delay)


# ╔══════════════════════════════════════════════════════════════╗
# ║                    MEMORY MANAGER                            ║
# ╚══════════════════════════════════════════════════════════════╝


def get_rss_mb() -> float:
    """Get current RSS (not peak) in MB."""
    try:
        # Linux (Render) — read current RSS from /proc
        with open("/proc/self/statm") as f:
            pages = int(f.read().split()[1])
            return (pages * os.sysconf("SC_PAGE_SIZE")) / (1024 * 1024)
    except Exception:
        pass
    try:
        # macOS fallback (development)
        import resource

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return rss / (1024 * 1024)
        return rss / 1024
    except Exception:
        return 0


def memory_watchdog():
    """Monitor RSS and take action when approaching OOM limits."""
    early_synced = False
    while True:
        time.sleep(MEMORY_CHECK_INTERVAL)
        rss = get_rss_mb()
        uptime_min = (time.time() - BOOT_TIME) / 60

        with key_lock:
            now = time.time()
            active_keys = sum(
                1 for s in key_state.values() if now >= s["cooldown_until"]
            )
            total_ok = sum(s["total_ok"] for s in key_state.values())
            total_fail = sum(s["total_fail"] for s in key_state.values())

        print(
            f"[MEM] {rss:.0f}/{MEMORY_LIMIT_MB} MB | "
            f"Keys: {active_keys}/{len(nvidia_keys)} ready | "
            f"API: {total_ok} ok / {total_fail} fail | "
            f"Sync: ↑{sync_stats['uploads']} ↓{sync_stats['downloads']} | "
            f"Up: {uptime_min:.0f}m",
            flush=True,
        )

        if rss > MEMORY_CRITICAL_MB:
            print(
                f"[MEM] 🔴 CRITICAL ({rss:.0f} MB) — forcing GC + sync",
                flush=True,
            )
            gc.collect()
            sync_upload_changed()
        elif rss > MEMORY_WARNING_MB and not early_synced:
            early_synced = True
            print(
                f"[MEM] ⚠️ WARNING ({rss:.0f} MB) — early sync triggered",
                flush=True,
            )
            sync_upload_changed()
        elif rss <= MEMORY_WARNING_MB:
            early_synced = False


# ╔══════════════════════════════════════════════════════════════╗
# ║                      FLASK ROUTES                            ║
# ╚══════════════════════════════════════════════════════════════╝

port = int(os.environ.get("PORT", 8080))


@app.route("/")
def index():
    return "Hermes Status: ONLINE", 200, {"Content-Type": "text/plain"}


@app.route("/health")
def health():
    """Rich JSON health dashboard — memory, keys, sync, hermes status."""
    rss = get_rss_mb()
    uptime = time.time() - BOOT_TIME
    now = time.time()

    # Per-key detail
    keys_detail = []
    with key_lock:
        for i in sorted(key_state.keys()):
            s = key_state[i]
            cd_left = max(0, s["cooldown_until"] - now)
            keys_detail.append(
                {
                    "key": f"#{i+1}",
                    "status": "ready" if cd_left == 0 else f"cooldown {cd_left:.0f}s",
                    "ok": s["total_ok"],
                    "fail": s["total_fail"],
                    "streak_429": s["consecutive_429"],
                    "streak_403": s["consecutive_403"],
                }
            )
        active_keys = sum(1 for d in keys_detail if d["status"] == "ready")

    # Sync detail — what files are being tracked
    tracked_categories = {}
    for rel_path in sync_manifest:
        if rel_path == "_manifest.json":
            continue
        cat = rel_path.split("/")[0] if "/" in rel_path else "(root)"
        tracked_categories[cat] = tracked_categories.get(cat, 0) + 1

    status = {
        "status": "online",
        "uptime_seconds": round(uptime),
        "uptime_human": f"{uptime/3600:.1f}h",
        "memory": {
            "rss_mb": round(rss, 1),
            "limit_mb": MEMORY_LIMIT_MB,
            "usage_pct": round((rss / MEMORY_LIMIT_MB) * 100, 1)
            if MEMORY_LIMIT_MB
            else 0,
        },
        "hermes": hermes_status,
        "nvidia_keys": {
            "total": len(nvidia_keys),
            "active": active_keys,
            "detail": keys_detail,
        },
        "sync": {
            "connected": supabase_ready,
            "total_uploads": sync_stats["uploads"],
            "total_downloads": sync_stats["downloads"],
            "errors": sync_stats["errors"],
            "last_sync": sync_stats["last_sync"],
            "files_tracked": len(sync_manifest),
            "categories": tracked_categories,
            "recent_files": sync_stats.get("files_synced", []),
        },
    }
    return json.dumps(status, indent=2, default=str), 200, {"Content-Type": "application/json"}


# --- Fallback routes for Ollama / misc backend probes ---
# Hermes probes these on startup to detect what kind of API it's talking to.
# Without these, they 404 and flood the logs.

@app.route("/api/v1/models", methods=["GET"])
@app.route("/api/tags", methods=["GET"])
def ollama_models_fallback():
    """Return an empty model list so Ollama-style probes don't 404."""
    return json.dumps({"models": []}), 200, {"Content-Type": "application/json"}


@app.route("/api/show", methods=["POST"])
def ollama_show_fallback():
    """Return a stub response for Ollama model-info probes."""
    return json.dumps({"details": {"parent_model": ""}}), 200, {"Content-Type": "application/json"}


@app.route("/v1/props", methods=["GET"])
@app.route("/props", methods=["GET"])
def props_fallback():
    return json.dumps({}), 200, {"Content-Type": "application/json"}


@app.route("/version", methods=["GET"])
def version_fallback():
    return json.dumps({"version": "proxy-1.0"}), 200, {"Content-Type": "application/json"}


@app.route("/v1/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
def proxy(path):
    """NVIDIA API proxy with adaptive key rotation and cooldowns."""

    # Short-circuit backend detection probes — don't waste key rotations
    if _is_probe(path):
        return "Not Found", 404

    if not nvidia_keys:
        return "No NVIDIA keys configured", 500

    target_url = f"https://integrate.api.nvidia.com/v1/{path}"
    headers = {
        k: v
        for k, v in request.headers
        if k.lower() not in ("host", "authorization")
    }
    headers["Connection"] = "close"  # prevent stale socket reuse after idle periods
    excluded = frozenset(
        {"content-encoding", "content-length", "transfer-encoding", "connection"}
    )

    max_attempts = len(nvidia_keys)
    tried_indices = set()
    last_resp = None

    for attempt in range(max_attempts):
        key_val, idx = get_next_key(tried_indices)
        if idx is None:
            break
        tried_indices.add(idx)

        headers["Authorization"] = f"Bearer {key_val}"
        log_path = path.split("/")[-1]  # shorten for logging
        print(
            f"[PROXY] {attempt+1}/{max_attempts} → {log_path} (key #{idx+1})",
            flush=True,
        )

        try:
            resp = http_requests.request(
                method=request.method,
                url=target_url,
                headers=headers,
                data=request.get_data(),
                cookies=request.cookies,
                allow_redirects=False,
                stream=True,
                timeout=KEY_REQUEST_TIMEOUT,
            )
        except http_requests.Timeout:
            print(
                f"[PROXY] Key #{idx+1} timed out ({KEY_REQUEST_TIMEOUT}s)",
                flush=True,
            )
            continue
        except http_requests.ConnectionError as e:
            print(f"[PROXY] Connection error: {e}", flush=True)
            continue

        if resp.status_code in (401, 403, 429) or resp.status_code >= 500:
            report_key_result(idx, resp.status_code, resp.headers)
            if last_resp:
                last_resp.close()
            last_resp = resp
            continue

        report_key_result(idx, resp.status_code, resp.headers)
        if last_resp:
            last_resp.close()

        resp_headers = [
            (n, v) for n, v in resp.raw.headers.items() if n.lower() not in excluded
        ]

        def generate_success():
            try:
                for chunk in resp.iter_content(chunk_size=4096):
                    yield chunk
            finally:
                resp.close()

        return Response(generate_success(), resp.status_code, resp_headers)

    if last_resp:
        resp_headers = [
            (n, v) for n, v in last_resp.raw.headers.items() if n.lower() not in excluded
        ]

        def generate_failure():
            try:
                for chunk in last_resp.iter_content(chunk_size=4096):
                    yield chunk
            finally:
                last_resp.close()

        return Response(generate_failure(), last_resp.status_code, resp_headers)

    return "All NVIDIA keys exhausted or on cooldown — try again shortly", 503


# ╔══════════════════════════════════════════════════════════════╗
# ║                     SIGNAL HANDLERS                          ║
# ╚══════════════════════════════════════════════════════════════╝


def shutdown(signum, frame):
    sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
    print(f"[SHUTDOWN] Received {sig_name} — graceful shutdown...", flush=True)

    _kill_hermes()
    print("[SHUTDOWN] Hermes stopped", flush=True)

    if supabase_ready:
        print("[SHUTDOWN] Saving ALL state to Supabase...", flush=True)
        sync_upload_all()
        print("[SHUTDOWN] ✅ State saved", flush=True)

    sys.exit(0)


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


# ╔══════════════════════════════════════════════════════════════╗
# ║                        STARTUP                               ║
# ╚══════════════════════════════════════════════════════════════╝

# --- 1. Restore state from Supabase FIRST (state.db, skills, memories, etc.) ---
sync_download_all()

# --- 2. Write .env from Render env vars (always overwrites — env vars are source of truth) ---
dotenv_lines = []
telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN")
if telegram_token:
    dotenv_lines.append(f"TELEGRAM_BOT_TOKEN={telegram_token}")

for i in range(1, 7):
    val = os.environ.get(f"NVIDIA_NIM_KEY_{i}")
    if val:
        dotenv_lines.append(f"NVIDIA_NIM_KEY_{i}={val}")

honcho_key = os.environ.get("HONCHO_API_KEY")
if honcho_key:
    dotenv_lines.append(f"HONCHO_API_KEY={honcho_key}")
    honcho_dir = Path.home() / ".honcho"
    honcho_dir.mkdir(parents=True, exist_ok=True)
    honcho_config = honcho_dir / "config.json"
    if not honcho_config.exists():
        honcho_config.write_text(json.dumps({"enabled": True}))

# Email and GitHub credentials
for evar in (
    "EMAIL_ADDRESS",
    "EMAIL_PASSWORD",
    "EMAIL_IMAP_HOST",
    "EMAIL_IMAP_PORT",
    "EMAIL_SMTP_HOST",
    "EMAIL_SMTP_PORT",
    "GITHUB_TOKEN",
    "GH_TOKEN",
):
    val = os.environ.get(evar)
    if val:
        dotenv_lines.append(f"{evar}={val}")

# Agent gateway timeout — default 600s is too aggressive for complex tasks
dotenv_lines.append("HERMES_AGENT_TIMEOUT=1800")

(HERMES_HOME / ".env").write_text("\n".join(dotenv_lines) + "\n")

# --- 3. Copy config.yaml from repo if not already present ---
config_src = Path(__file__).parent / "config.yaml"
config_dst = HERMES_HOME / "config.yaml"
if config_src.exists() and not config_dst.exists():
    config_dst.write_text(config_src.read_text())

# Apply network/proxy overrides to config.yaml before starting Hermes
if config_dst.exists():
    try:
        with open(config_dst, "r") as f:
            cfg = yaml.safe_load(f) or {}
            
        # 1. Force IPv4 for python-telegram-bot to avoid AF_INET6 timeouts on Docker hosts
        cfg.setdefault("network", {})["force_ipv4"] = True
        
        # 2. Inject Telegram Proxy URL if provided
        tg_proxy = os.environ.get("TELEGRAM_API_URL")
        if tg_proxy:
            tg_proxy = tg_proxy.rstrip("/")
            if not tg_proxy.endswith("/bot"):
                tg_proxy += "/bot"
            cfg.setdefault("telegram", {}).setdefault("extra", {})["base_url"] = tg_proxy

        # 3. Extend gateway inactivity timeout (default 600s is too short)
        cfg.setdefault("agent", {})["gateway_timeout"] = 1800

        with open(config_dst, "w") as f:
            yaml.dump(cfg, f)
    except Exception as e:
        print(f"[BOOT] Failed to modify config.yaml: {e}")

# --- 4. Build Hermes subprocess environment ---
hermes_env = os.environ.copy()
hermes_env["HERMES_HOME"] = str(HERMES_HOME)
hermes_env["PYTHONUNBUFFERED"] = "1"

proxy_url = f"http://127.0.0.1:{port}/v1"
hermes_env["NVIDIA_BASE_URL"] = proxy_url
hermes_env["NVIDIA_API_BASE"] = proxy_url
hermes_env["OPENAI_BASE_URL"] = proxy_url

if "NVIDIA_NIM_KEY_1" in os.environ:
    hermes_env.setdefault("NVIDIA_API_KEY", os.environ["NVIDIA_NIM_KEY_1"])

# --- 5. Boot summary ---
if os.environ.get("DATABASE_URL"):
    print("[BOOT] Supabase PostgreSQL: Enabled", flush=True)
else:
    print("[BOOT] Supabase PostgreSQL: Not configured", flush=True)

files_in_hermes = list(HERMES_HOME.rglob("*"))
dirs_in_hermes = set()
for f in files_in_hermes:
    if f.is_file():
        rel = f.relative_to(HERMES_HOME)
        if len(rel.parts) > 1:
            dirs_in_hermes.add(rel.parts[0])

print(f"[BOOT] NVIDIA keys: {len(nvidia_keys)} loaded", flush=True)
print(f"[BOOT] Hermes home: {HERMES_HOME}", flush=True)
print(
    f"[BOOT] State: {sum(1 for f in files_in_hermes if f.is_file())} files "
    f"in {len(dirs_in_hermes)} directories",
    flush=True,
)
if dirs_in_hermes:
    print(f"[BOOT]   ↳ dirs: {', '.join(sorted(dirs_in_hermes))}", flush=True)
print(
    f"[BOOT] Sync interval: {SYNC_INTERVAL_SECS}s | "
    f"Memory limit: {MEMORY_LIMIT_MB} MB | "
    f"Watchdog: {HERMES_MAX_RESTARTS} max restarts",
    flush=True,
)

# --- 6. Start background threads ---

# Hermes watchdog (with auto-restart)
threading.Thread(
    target=run_hermes_watchdog,
    args=(hermes_env,),
    daemon=True,
    name="hermes-watchdog",
).start()

# Periodic state sync
if supabase_ready:

    def _periodic_sync_loop():
        while True:
            time.sleep(SYNC_INTERVAL_SECS)
            try:
                sync_upload_changed()
            except Exception as e:
                print(f"[SYNC] Periodic sync error: {e}", flush=True)

    threading.Thread(
        target=_periodic_sync_loop, daemon=True, name="periodic-sync"
    ).start()
    print("[BOOT] Periodic sync: ON (every 10 min)", flush=True)

# Memory monitor
threading.Thread(
    target=memory_watchdog, daemon=True, name="memory-monitor"
).start()

# --- Keep-alive self-ping (prevents HF Spaces from sleeping) ---
KEEP_ALIVE_INTERVAL = 240  # seconds (4 minutes)

def _keep_alive_loop():
    """Ping our own public URL to prevent Hugging Face from sleeping the Space."""
    hf_token = os.environ.get("HF_TOKEN", "")
    space_host = os.environ.get("SPACE_HOST", "")  # e.g. "sidon1-hermes-agenta-slam.hf.space"

    if not space_host:
        print("[KEEPALIVE] ⚠ SPACE_HOST not set — self-ping disabled", flush=True)
        return
    if not hf_token:
        print("[KEEPALIVE] ⚠ HF_TOKEN not set — self-ping disabled (Space may sleep)", flush=True)
        return

    ping_url = f"https://{space_host}/"
    headers = {"Authorization": f"Bearer {hf_token}"}
    print(f"[KEEPALIVE] ✅ Self-ping enabled every {KEEP_ALIVE_INTERVAL}s → {ping_url}", flush=True)

    while True:
        time.sleep(KEEP_ALIVE_INTERVAL)
        try:
            r = http_requests.get(ping_url, headers=headers, timeout=15)
            # Silently succeed — only log errors
            if r.status_code not in (200, 302):
                print(f"[KEEPALIVE] ⚠ Ping returned HTTP {r.status_code}", flush=True)
        except Exception as e:
            print(f"[KEEPALIVE] ⚠ Ping failed: {e}", flush=True)

threading.Thread(
    target=_keep_alive_loop, daemon=True, name="keep-alive"
).start()

print("[BOOT] ✅ All systems initialized — Hermes Agent is starting", flush=True)

# --- Entry point ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=port)
