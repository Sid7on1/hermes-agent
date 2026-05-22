#!/usr/bin/env python3
"""
app.py — Hermes Proxy · Watchdog · State Sync Engine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Production-hardened rewrite. Fixes:
  • Signal handler → non-blocking shutdown thread (no deadlock risk)
  • All shared state gated behind appropriate locks
  • time.monotonic() throughout for cooldown/uptime timing
  • Shared requests.Session with a real connection pool
  • Streaming generator captures value not closure variable
  • All response objects guaranteed-closed via try/finally
  • sync_manifest/sync_stats/hermes_status all thread-safe
  • Key cooldown jitter prevents thundering-herd re-429
  • Watchdog uses threading.Event for clean sleep + wakeup
  • WAL checkpoint owns its own connection + proper timeout
  • SYNC_EXCLUDE_SUFFIXES no longer contains dead -wal/-shm entries
  • Health endpoint snapshots all shared state under lock before serialising
  • keep-alive / periodic-sync threads exit cleanly on shutdown
  • mimetypes.guess_type for correct Content-Type on upload
  • Per-file upload timeout scaled by file size
  • dataclass KeyState — no raw dict mutation bugs
  • Structured logging replaces ad-hoc print() calls
  • atexit + signal both route to single idempotent _graceful_shutdown()
  • deque(maxlen=N) replaces list[-10:] slice assignment race
"""

from __future__ import annotations

import atexit
import gc
import json
import logging
import mimetypes
import os
import random
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import requests
from requests.adapters import HTTPAdapter
import yaml
from flask import Flask, Response, request as flask_req

# ══════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)-14s] %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("hermes")

# ══════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════

HERMES_HOME = Path.home() / ".hermes"
HERMES_HOME.mkdir(parents=True, exist_ok=True)

SUPABASE_BUCKET        = "hermes-state"
SYNC_INTERVAL_SECS     = 600
MAX_SYNC_FILE_MB       = 50
SYNC_EXCLUDE_DIRS      = frozenset({"__pycache__", ".git", "cache", "sandbox", "tmp"})
# Only real file-extension suffixes here — "-wal" / "-shm" are NOT extensions
# and would never match Path.suffix. They are handled separately below.
SYNC_EXCLUDE_SUFFIXES  = frozenset({".pyc", ".pyo", ".log", ".tmp"})
SYNC_EXCLUDE_TAILS     = ("-wal", "-shm")   # matched via str.endswith()

MEM_CHECK_INTERVAL     = 30            # seconds
MEM_WARNING_MB         = 6_000
MEM_CRITICAL_MB        = 8_000
MEM_LIMIT_MB           = 16_384

# KEY_COOLDOWN_BASE doubles each consecutive 429 (exponential back-off)
KEY_COOLDOWN_BASE      = 2.0           # seconds
KEY_COOLDOWN_MAX       = 60.0          # seconds
# Jitter prevents all cooled-down keys hammering together (thundering herd)
KEY_COOLDOWN_JITTER    = 1.0           # seconds (uniform random 0..KEY_COOLDOWN_JITTER)
KEY_TIMEOUT            = (10, 300)     # (connect_s, read_s)
# Maximum time to wait for the least-cold key before giving up immediately
KEY_MAX_COOLDOWN_WAIT  = 5.0           # seconds

WATCHDOG_START_DELAY   = 45            # seconds – Telegram poll lock expiry
WATCHDOG_MAX_RESTARTS  = 5             # within WATCHDOG_WINDOW
WATCHDOG_WINDOW        = 600           # seconds
WATCHDOG_BASE_DELAY    = 5             # restart backoff base (doubles each restart)

KEEP_ALIVE_INTERVAL    = 240           # seconds

PORT      = int(os.environ.get("PORT", 8080))
BOOT_MONO = time.monotonic()   # wall-clock-independent uptime reference

# ══════════════════════════════════════════════════════════════
#  SHARED HTTP SESSION  (connection-pooled, keep-alive)
# ══════════════════════════════════════════════════════════════

# One session for all outbound calls (Supabase + NVIDIA + keep-alive).
# max_retries=0 here because retry logic is implemented at a higher level.
_http = requests.Session()
_http.mount("https://", HTTPAdapter(pool_connections=16, pool_maxsize=64, max_retries=0))
_http.mount("http://",  HTTPAdapter(pool_connections=8,  pool_maxsize=32, max_retries=0))

# ══════════════════════════════════════════════════════════════
#  FLASK
# ══════════════════════════════════════════════════════════════

app = Flask(__name__)

# ══════════════════════════════════════════════════════════════
#  SHUTDOWN COORDINATION
#  All threads check _shutdown_event.is_set() and exit cleanly.
#  _graceful_shutdown() is idempotent — safe to call from multiple paths.
# ══════════════════════════════════════════════════════════════

_shutdown_event = threading.Event()
_shutdown_lock  = threading.Lock()   # ensures _graceful_shutdown() runs exactly once
_shutdown_done  = False

# ══════════════════════════════════════════════════════════════
#  SUPABASE SYNC ENGINE
# ══════════════════════════════════════════════════════════════

_sb_url  = os.environ.get("SUPABASE_URL", "").rstrip("/")
_sb_key  = os.environ.get("SUPABASE_KEY", "")
_sb_hdrs: dict[str, str] = (
    {"Authorization": f"Bearer {_sb_key}", "apikey": _sb_key}
    if (_sb_url and _sb_key) else {}
)
_sb_ok   = False  # confirmed True after _probe_supabase()

# Single lock guards both _sync_manifest and _sync_stats.
# Never hold this lock while doing I/O — snapshot first, I/O second.
_sync_lock     = threading.Lock()
_sync_manifest: dict[str, dict] = {}
_sync_stats: dict = {
    "uploads":      0,
    "downloads":    0,
    "errors":       0,
    "last_sync":    None,
    "recent_files": deque(maxlen=10),   # thread-safe append, bounded
}


def _probe_supabase() -> bool:
    """Verify Supabase connectivity. Sets module-level _sb_ok."""
    global _sb_ok
    if not (_sb_url and _sb_key):
        log.info("SYNC  Supabase not configured — SUPABASE_URL/KEY missing")
        return False
    try:
        r = _http.get(
            f"{_sb_url}/storage/v1/bucket/{SUPABASE_BUCKET}",
            headers=_sb_hdrs, timeout=10,
        )
        _sb_ok = r.status_code == 200
        if _sb_ok:
            log.info("SYNC  ✅ Supabase Storage connected")
        else:
            log.warning("SYNC  ⚠ Supabase bucket check: HTTP %s", r.status_code)
    except Exception as exc:
        log.error("SYNC  ❌ Supabase unreachable: %s", exc)
        _sb_ok = False
    return _sb_ok


# ── filter helpers ─────────────────────────────────────────────

def _should_sync(rel: str) -> bool:
    p = Path(rel)
    if any(part in SYNC_EXCLUDE_DIRS for part in p.parts):
        return False
    if p.suffix in SYNC_EXCLUDE_SUFFIXES:
        return False
    if any(p.name.endswith(t) for t in SYNC_EXCLUDE_TAILS):
        return False
    return True


def _local_files() -> dict[str, dict]:
    """Enumerate syncable files under HERMES_HOME with size+mtime metadata."""
    limit = MAX_SYNC_FILE_MB * 1_048_576
    out: dict[str, dict] = {}
    for fp in HERMES_HOME.rglob("*"):
        if not fp.is_file():
            continue
        rel = str(fp.relative_to(HERMES_HOME))
        if not _should_sync(rel):
            continue
        try:
            st = fp.stat()
        except OSError:
            continue
        if st.st_size > limit:
            continue
        out[rel] = {"size": st.st_size, "mtime": st.st_mtime}
    return out


def _mime(rel: str) -> str:
    """Best-effort MIME type for a relative path."""
    t, _ = mimetypes.guess_type(rel)
    if t:
        return t
    return {
        ".yaml": "text/plain", ".yml": "text/plain",
        ".md":   "text/plain", ".env": "text/plain",
        ".json": "application/json",
    }.get(Path(rel).suffix.lower(), "application/octet-stream")


# ── low-level Supabase REST helpers ───────────────────────────

def _sb_get(path: str, timeout: int = 30) -> requests.Response | None:
    if not _sb_ok:
        return None
    try:
        return _http.get(
            f"{_sb_url}/storage/v1/object/{SUPABASE_BUCKET}/{path}",
            headers=_sb_hdrs, timeout=timeout,
        )
    except Exception as exc:
        log.warning("SYNC  GET %s: %s", path, exc)
        return None


def _sb_put(path: str, data: bytes, ct: str, timeout: int = 60) -> bool:
    if not _sb_ok:
        return False
    try:
        hdrs = {**_sb_hdrs, "Content-Type": ct, "x-upsert": "true"}
        r = _http.post(
            f"{_sb_url}/storage/v1/object/{SUPABASE_BUCKET}/{path}",
            headers=hdrs, data=data, timeout=timeout,
        )
        if r.status_code in (200, 201):
            return True
        log.warning("SYNC  PUT %s: HTTP %s — %.200s", path, r.status_code, r.text)
        return False
    except Exception as exc:
        log.warning("SYNC  PUT %s: %s", path, exc)
        return False


# ── single-file operations ────────────────────────────────────

def _download_one(rel: str) -> bool:
    r = _sb_get(rel)
    if r is None:
        return False
    if r.status_code == 200:
        dest = HERMES_HOME / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(r.content)
        with _sync_lock:
            _sync_stats["downloads"] += 1
        return True
    if r.status_code != 404:
        log.warning("SYNC  download %s: HTTP %s", rel, r.status_code)
        with _sync_lock:
            _sync_stats["errors"] += 1
    return False


def _upload_one(rel: str) -> bool:
    local = HERMES_HOME / rel
    if not local.exists():
        return False
    data = local.read_bytes()
    # Scale timeout by file size: 60s base + 1s per MB
    size_mb = len(data) / 1_048_576
    timeout = int(60 + size_mb)
    ok = _sb_put(rel, data, _mime(rel), timeout=timeout)
    with _sync_lock:
        if ok:
            _sync_stats["uploads"] += 1
        else:
            _sync_stats["errors"] += 1
    return ok


# ── manifest helpers ──────────────────────────────────────────

def _load_manifest() -> dict:
    r = _sb_get("_manifest.json", timeout=15)
    if r and r.status_code == 200:
        try:
            return r.json()
        except ValueError:
            pass
    return {}


def _save_manifest() -> None:
    with _sync_lock:
        snapshot = dict(_sync_manifest)   # snapshot before I/O
    data = json.dumps(snapshot, indent=2, default=str).encode()
    _sb_put("_manifest.json", data, "application/json", timeout=15)


def _wal_checkpoint() -> None:
    """Flush SQLite WAL so state.db is self-contained before upload."""
    db = HERMES_HOME / "state.db"
    if not db.exists():
        return
    try:
        # timeout= is the connection-acquisition timeout, not query timeout.
        conn = sqlite3.connect(str(db), timeout=10)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            conn.close()
    except Exception as exc:
        log.warning("SYNC  WAL checkpoint warning: %s", exc)


# ── high-level sync operations ────────────────────────────────

def sync_download_all() -> None:
    """
    Restore full state from Supabase.
    Called once at startup before any background threads are running —
    no locking needed on _sync_manifest here.
    """
    global _sync_manifest
    if not _sb_ok:
        return
    log.info("SYNC  📥 Restoring state from Supabase...")
    manifest = _load_manifest()
    if not manifest:
        log.info("SYNC  No manifest — first deployment or empty bucket")
        for f in ("state.db", "config.yaml", ".env"):
            if _download_one(f):
                log.info("SYNC    ↳ legacy: %s", f)
        return

    _sync_manifest = {k: v for k, v in manifest.items() if k != "_manifest.json"}
    ok = fail = 0
    cats: dict[str, int] = {}
    for rel in sorted(_sync_manifest):
        if _download_one(rel):
            ok += 1
            cat = rel.split("/")[0] if "/" in rel else "(root)"
            cats[cat] = cats.get(cat, 0) + 1
        else:
            fail += 1
    detail = ", ".join(f"{c}:{n}" for c, n in sorted(cats.items()))
    log.info(
        "SYNC  📥 Restored %d files (%d failures)%s",
        ok, fail, f" — {detail}" if detail else "",
    )


def sync_upload_changed() -> None:
    """
    Incremental sync — only files whose mtime or size changed since the
    last upload.  Takes a manifest snapshot before doing any I/O so the
    lock is never held across network calls.
    """
    if not _sb_ok:
        return
    _wal_checkpoint()
    local = _local_files()

    # Snapshot manifest while locked, then release before I/O
    with _sync_lock:
        prev = dict(_sync_manifest)

    now_iso  = datetime.now(timezone.utc).isoformat()
    uploaded: list[str] = []

    for rel, info in local.items():
        p = prev.get(rel, {})
        if p.get("mtime") == info["mtime"] and p.get("size") == info["size"]:
            continue  # unchanged
        if _upload_one(rel):
            entry = {**info, "synced_at": now_iso}
            with _sync_lock:
                _sync_manifest[rel] = entry
            uploaded.append(rel)

    if uploaded:
        _save_manifest()
        with _sync_lock:
            _sync_stats["last_sync"] = now_iso
            _sync_stats["recent_files"].extend(uploaded)
        log.info("SYNC  📤 Incremental: %d files", len(uploaded))


def sync_upload_all() -> None:
    """Force-upload every file (used on shutdown — correctness > speed)."""
    if not _sb_ok:
        return
    _wal_checkpoint()
    local    = _local_files()
    now_iso  = datetime.now(timezone.utc).isoformat()
    n        = 0

    for rel, info in local.items():
        if _upload_one(rel):
            with _sync_lock:
                _sync_manifest[rel] = {**info, "synced_at": now_iso}
            n += 1

    _save_manifest()
    with _sync_lock:
        _sync_stats["last_sync"] = now_iso
    log.info("SYNC  📤 Full sync: %d files", n)


# ══════════════════════════════════════════════════════════════
#  NVIDIA KEY MANAGER
# ══════════════════════════════════════════════════════════════

@dataclass
class _KeyState:
    cooldown_until:  float = 0.0    # monotonic timestamp
    consecutive_429: int   = 0
    consecutive_403: int   = 0
    total_ok:        int   = 0
    total_fail:      int   = 0
    last_used:       float = field(default_factory=time.monotonic)

    def ready(self) -> bool:
        return time.monotonic() >= self.cooldown_until

    def cooldown_remaining(self) -> float:
        return max(0.0, self.cooldown_until - time.monotonic())


# Load all NVIDIA keys — deduplicate while preserving order
_nvidia_keys: list[str] = []
for _i in range(1, 7):
    _v = os.environ.get(f"NVIDIA_NIM_KEY_{_i}", "")
    if _v and _v not in _nvidia_keys:
        _nvidia_keys.append(_v)
_v = os.environ.get("NVIDIA_API_KEY", "")
if _v and _v not in _nvidia_keys:
    _nvidia_keys.append(_v)

_key_lock  = threading.Lock()
_key_state = [_KeyState() for _ in _nvidia_keys]
_key_rr    = 0   # round-robin cursor (protected by _key_lock)

# Paths the NVIDIA backend never serves — short-circuit them
# to avoid wasting key quota on probe traffic from Hermes.
_PROBE_PATHS = frozenset({"props", "version"})


def _is_probe(path: str) -> bool:
    return path in _PROBE_PATHS or path.startswith("api/")


def get_next_key(exclude: set[int] | None = None) -> tuple[str | None, int | None]:
    """
    Round-robin key selection.
    Pass 1: return the first ready key (not on cooldown).
    Pass 2: if all keys are on cooldown, return the one whose cooldown
            expires soonest.  The caller is responsible for sleeping if
            cooldown_remaining() > KEY_MAX_COOLDOWN_WAIT.
    Returns (None, None) if no keys at all or all excluded.
    """
    global _key_rr
    ex = exclude or set()
    if not _nvidia_keys:
        return None, None

    with _key_lock:
        n          = len(_nvidia_keys)
        now        = time.monotonic()
        candidates = [i for i in range(n) if i not in ex]
        if not candidates:
            return None, None

        # Pass 1 — ready keys
        for _ in range(n):
            idx      = _key_rr % n
            _key_rr += 1
            if idx in ex:
                continue
            if now >= _key_state[idx].cooldown_until:
                _key_state[idx].last_used = now
                return _nvidia_keys[idx], idx

        # Pass 2 — all on cooldown; pick soonest
        idx = min(candidates, key=lambda i: _key_state[i].cooldown_until)
        _key_state[idx].last_used = now
        return _nvidia_keys[idx], idx


def report_key_result(idx: int, status: int, resp_headers=None) -> None:
    """Update per-key state after an upstream API call."""
    now = time.monotonic()
    with _key_lock:
        s = _key_state[idx]
        if 200 <= status < 300:
            s.total_ok         += 1
            s.consecutive_429   = 0
            s.consecutive_403   = 0
            s.cooldown_until    = 0.0
            return

        s.total_fail += 1

        if status == 429:
            s.consecutive_429 += 1
            # Honour server-side Retry-After when present
            cooldown = None
            if resp_headers:
                for h in ("retry-after", "Retry-After", "x-ratelimit-reset-after"):
                    v = (resp_headers.get(h) or "").strip()
                    if v:
                        try:
                            cooldown = float(v)
                            break
                        except ValueError:
                            pass
            if cooldown is None:
                cooldown = min(
                    KEY_COOLDOWN_BASE * (2 ** (s.consecutive_429 - 1)),
                    KEY_COOLDOWN_MAX,
                )
            # Jitter prevents all keys hammering together after cooldown expires
            jitter             = random.uniform(0.0, KEY_COOLDOWN_JITTER)
            s.cooldown_until   = now + cooldown + jitter
            log.warning(
                "KEY   #%d → 429 cooldown %.1fs + %.1fj (streak %d)",
                idx + 1, cooldown, jitter, s.consecutive_429,
            )

        elif status == 403:
            s.consecutive_403 += 1
            s.cooldown_until   = now + 2.0
            log.warning("KEY   #%d → 403 cooldown 2s (streak %d)", idx + 1, s.consecutive_403)

        elif status == 401:
            s.cooldown_until = now + 5.0
            log.warning("KEY   #%d → 401 cooldown 5s", idx + 1)

        elif status in (502, 504):
            # Network-level errors — short cooldown, likely transient
            s.cooldown_until = now + 2.0

        elif status >= 500:
            s.cooldown_until = now + 3.0
            log.warning("KEY   #%d → server error %d cooldown 3s", idx + 1, status)


# ══════════════════════════════════════════════════════════════
#  HERMES WATCHDOG
# ══════════════════════════════════════════════════════════════

_hermes_proc: subprocess.Popen | None = None
_hermes_lock                           = threading.Lock()
# _hermes_status is read from the health endpoint — needs its own lock
_hermes_status_lock = threading.Lock()
_hermes_status = {
    "state": "initializing", "pid": None,
    "started_at": None, "restarts": 0, "last_exit_code": None,
}
_hermes_restarts: deque[float] = deque()   # monotonic timestamps of recent crashes


def _update_status(**kw) -> None:
    with _hermes_status_lock:
        _hermes_status.update(kw)


def _setsid() -> None:
    """Give the child process its own session so SIGTERM reaches the group."""
    try:
        os.setsid()
    except OSError:
        pass


def _start_hermes(env: dict) -> None:
    global _hermes_proc
    _hermes_proc = subprocess.Popen(
        ["hermes", "gateway"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        preexec_fn=_setsid,
    )
    _update_status(
        state="running",
        pid=_hermes_proc.pid,
        started_at=datetime.now(timezone.utc).isoformat(),
    )
    log.info("WATCH 🚀 Hermes started (PID %d)", _hermes_proc.pid)


def _kill_hermes() -> None:
    """Terminate the entire Hermes process group, SIGTERM then SIGKILL."""
    with _hermes_lock:
        proc = _hermes_proc
    if proc is None or proc.poll() is not None:
        return
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGTERM)
    except (OSError, ProcessLookupError, PermissionError):
        try:
            proc.terminate()
        except OSError:
            pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            pgid = os.getpgid(proc.pid)
            os.killpg(pgid, signal.SIGKILL)
        except (OSError, ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except OSError:
                pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def run_hermes_watchdog(env: dict) -> None:
    """
    Main watchdog loop.  Starts Hermes and auto-restarts on crash.
    Exits on:
      • _shutdown_event being set
      • clean exit (rc == 0)
      • crash-loop detection (too many restarts)
    """
    global _hermes_proc

    log.info("WATCH Waiting %ds for Telegram poll lock to expire...", WATCHDOG_START_DELAY)
    _update_status(state="waiting")
    # Wait using the shutdown event — exits early if shutdown is triggered
    if _shutdown_event.wait(timeout=WATCHDOG_START_DELAY):
        log.info("WATCH Shutdown signalled during startup delay — exiting")
        return

    while not _shutdown_event.is_set():
        # ── Crash-loop guard ────────────────────────────────────────
        now = time.monotonic()
        while _hermes_restarts and now - _hermes_restarts[0] > WATCHDOG_WINDOW:
            _hermes_restarts.popleft()

        if len(_hermes_restarts) >= WATCHDOG_MAX_RESTARTS:
            _update_status(state="crash_loop")
            log.error(
                "WATCH ❌ Crash loop: %d restarts in %ds — giving up. Check logs.",
                WATCHDOG_MAX_RESTARTS, WATCHDOG_WINDOW,
            )
            sync_upload_changed()
            return

        # ── Start process ───────────────────────────────────────────
        with _hermes_lock:
            _start_hermes(env)
        proc = _hermes_proc

        # Stream stdout until the process exits or shutdown is requested
        if proc and proc.stdout:
            for line in proc.stdout:
                if _shutdown_event.is_set():
                    break
                # Write directly to avoid logging overhead for every line
                sys.stdout.write(f"[HERMES] {line}")
                sys.stdout.flush()

        rc = proc.wait() if proc else -1
        _update_status(state="exited", last_exit_code=rc)

        if _shutdown_event.is_set() or rc == 0:
            log.info("WATCH Hermes exited (code %d)", rc)
            break

        # ── Restart with exponential back-off ───────────────────────
        _hermes_restarts.append(time.monotonic())
        n_restarts = len(_hermes_restarts)
        _update_status(restarts=_hermes_status["restarts"] + 1)
        delay   = min(WATCHDOG_BASE_DELAY * (2 ** (n_restarts - 1)), 60)
        reason  = "OOM-killed" if rc in (-9, 137) else f"code {rc}"
        log.warning(
            "WATCH ⚠ Hermes crashed (%s) — restart %d/%d in %ds",
            reason, n_restarts, WATCHDOG_MAX_RESTARTS, delay,
        )
        sync_upload_changed()
        # Sleep respects shutdown — wakes immediately if shutdown fires
        if _shutdown_event.wait(timeout=delay):
            break

    log.info("WATCH thread exiting")


# ══════════════════════════════════════════════════════════════
#  MEMORY MONITOR
# ══════════════════════════════════════════════════════════════

def _rss_mb() -> float:
    """
    Combined RSS (proxy + Hermes agent) in megabytes via /proc/<pid>/statm.
    Fails gracefully — returns 0.0 if /proc is unavailable.
    """
    page_bytes = os.sysconf("SC_PAGE_SIZE")

    def _read(pid: int) -> float:
        try:
            with open(f"/proc/{pid}/statm") as fh:
                return int(fh.read().split()[1]) * page_bytes / 1_048_576
        except OSError:
            return 0.0

    total = _read(os.getpid())
    with _hermes_lock:
        proc = _hermes_proc
    if proc and proc.pid:
        total += _read(proc.pid)
    return total


def memory_watchdog() -> None:
    warned = False
    while not _shutdown_event.wait(timeout=MEM_CHECK_INTERVAL):
        rss    = _rss_mb()
        uptime = (time.monotonic() - BOOT_MONO) / 60

        with _key_lock:
            now_m  = time.monotonic()
            ready  = sum(1 for s in _key_state if now_m >= s.cooldown_until)
            tot_ok = sum(s.total_ok   for s in _key_state)
            tot_kf = sum(s.total_fail for s in _key_state)

        with _sync_lock:
            ups   = _sync_stats["uploads"]
            downs = _sync_stats["downloads"]

        log.info(
            "MEM   %d/%d MB | keys %d/%d ready | api %d ok %d fail | "
            "sync ↑%d ↓%d | up %.0fm",
            int(rss), MEM_LIMIT_MB, ready, len(_nvidia_keys),
            tot_ok, tot_kf, ups, downs, uptime,
        )

        if rss > MEM_CRITICAL_MB:
            log.warning("MEM   🔴 CRITICAL %.0f MB — GC + sync", rss)
            gc.collect()
            sync_upload_changed()
            warned = True
        elif rss > MEM_WARNING_MB and not warned:
            log.warning("MEM   ⚠ WARNING %.0f MB — early sync triggered", rss)
            sync_upload_changed()
            warned = True
        elif rss <= MEM_WARNING_MB:
            warned = False


# ══════════════════════════════════════════════════════════════
#  FLASK ROUTES
# ══════════════════════════════════════════════════════════════

# Hop-by-hop headers must be stripped before forwarding.
# We also strip encoding/length — the WSGI layer re-computes them.
_HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
    "content-encoding", "content-length", "host"
})


@app.route("/")
def index() -> Response:
    return Response("Hermes Status: ONLINE\n", 200, {"Content-Type": "text/plain"})


@app.route("/health")
def health() -> Response:
    """Comprehensive, race-free JSON health dashboard."""
    now    = time.monotonic()
    rss    = _rss_mb()
    uptime = now - BOOT_MONO

    # Snapshot key state under lock
    with _key_lock:
        keys_detail = []
        for i, s in enumerate(_key_state):
            cd = s.cooldown_remaining()
            keys_detail.append({
                "key":        f"#{i+1}",
                "status":     "ready" if cd == 0.0 else f"cooldown {cd:.0f}s",
                "ok":         s.total_ok,
                "fail":       s.total_fail,
                "streak_429": s.consecutive_429,
                "streak_403": s.consecutive_403,
            })
        active = sum(1 for d in keys_detail if d["status"] == "ready")

    # Snapshot sync state under lock
    with _sync_lock:
        sync_snap   = {k: v for k, v in _sync_stats.items() if k != "recent_files"}
        recent      = list(_sync_stats["recent_files"])
        mfst_snap   = dict(_sync_manifest)

    cats: dict[str, int] = {}
    for rel in mfst_snap:
        if rel == "_manifest.json":
            continue
        cat = rel.split("/")[0] if "/" in rel else "(root)"
        cats[cat] = cats.get(cat, 0) + 1

    # Snapshot hermes status under lock
    with _hermes_status_lock:
        hermes_snap = dict(_hermes_status)

    payload = {
        "status":  "online",
        "uptime":  {"seconds": round(uptime), "human": f"{uptime / 3600:.1f}h"},
        "memory":  {
            "rss_mb":    round(rss, 1),
            "limit_mb":  MEM_LIMIT_MB,
            "usage_pct": round(rss / MEM_LIMIT_MB * 100, 1),
        },
        "hermes":  hermes_snap,
        "keys":    {
            "total":  len(_nvidia_keys),
            "active": active,
            "detail": keys_detail,
        },
        "sync": {
            "connected":     _sb_ok,
            **sync_snap,
            "files_tracked": len(mfst_snap),
            "categories":    cats,
            "recent_files":  recent,
        },
    }
    return Response(
        json.dumps(payload, indent=2, default=str),
        200, {"Content-Type": "application/json"},
    )


# ── Ollama / backend-probe stub routes ────────────────────────
# Hermes probes these on startup to detect the backend type.
# Without stubs they 404 and flood the logs.

@app.route("/api/v1/models", methods=["GET"])
@app.route("/api/tags",      methods=["GET"])
def _stub_models() -> Response:
    return Response(json.dumps({"models": []}), 200, {"Content-Type": "application/json"})


@app.route("/api/show", methods=["POST"])
def _stub_show() -> Response:
    return Response(
        json.dumps({"details": {"parent_model": ""}}),
        200, {"Content-Type": "application/json"},
    )


@app.route("/v1/props", methods=["GET"])
@app.route("/props",    methods=["GET"])
def _stub_props() -> Response:
    return Response("{}", 200, {"Content-Type": "application/json"})


@app.route("/version", methods=["GET"])
def _stub_version() -> Response:
    return Response(json.dumps({"version": "proxy-1.0"}), 200, {"Content-Type": "application/json"})


# ── Main proxy ─────────────────────────────────────────────────

def _stream(resp: requests.Response, chunk_size: int | None = None) -> Generator[bytes, None, None]:
    """
    Yield chunks from a requests.Response and close it unconditionally.
    Defined at module level so there is no closure variable to capture —
    `resp` is passed by value as a default argument, making the binding
    permanent and correct even if the caller's local variable is rebound.
    """
    try:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            if chunk:
                yield chunk
    except Exception as exc:
        log.debug("PROXY stream interrupted: %s", exc)
    finally:
        resp.close()


@app.route(
    "/v1/<path:path>",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
def proxy(path: str) -> Response:
    """NVIDIA NIM API reverse proxy with adaptive per-key rate-limit handling."""

    # Backend detection probes — never forward, never waste key quota
    if _is_probe(path):
        return Response("Not Found\n", 404)

    if not _nvidia_keys:
        return Response("No NVIDIA keys configured\n", 503)

    if _shutdown_event.is_set():
        return Response("Service shutting down — retry shortly\n", 503,
                        {"Retry-After": "10"})

    target  = f"https://integrate.api.nvidia.com/v1/{path}"
    body    = flask_req.get_data()       # buffer once
    method  = flask_req.method

    # Forward headers — strip hop-by-hop and caller's auth header
    fwd_headers = {
        k: v for k, v in flask_req.headers
        if k.lower() not in _HOP_BY_HOP and k.lower() != "authorization"
    }

    max_tries  = len(_nvidia_keys)
    tried:     set[int] = set()
    last_resp: requests.Response | None = None

    try:
        for attempt in range(max_tries):
            key_val, idx = get_next_key(tried)
            if idx is None:
                break
            tried.add(idx)

            # If even the best available key is on long cooldown, fail fast
            # and let the client retry with Retry-After semantics.
            remaining = _key_state[idx].cooldown_remaining()
            if remaining > KEY_MAX_COOLDOWN_WAIT:
                log.warning(
                    "PROXY all keys on cooldown (best: %.0fs remaining) — 503",
                    remaining,
                )
                break
            if remaining > 0:
                time.sleep(remaining)

            fwd_headers["Authorization"] = f"Bearer {key_val}"
            log.info(
                "PROXY %d/%d → %s (key #%d)",
                attempt + 1, max_tries, path.rsplit("/", 1)[-1], idx + 1,
            )

            try:
                resp = _http.request(
                    method=method,
                    url=target,
                    headers=fwd_headers,
                    data=body,
                    cookies=flask_req.cookies,
                    allow_redirects=False,
                    stream=True,
                    timeout=KEY_TIMEOUT,
                )
            except requests.Timeout:
                log.warning("PROXY key #%d timed out", idx + 1)
                report_key_result(idx, 504)
                continue
            except requests.ConnectionError as exc:
                log.warning("PROXY connection error: %s", exc)
                report_key_result(idx, 502)
                continue

            if resp.status_code in (401, 403, 429) or resp.status_code >= 500:
                report_key_result(idx, resp.status_code, resp.headers)
                # Close the failed response before trying the next key
                if last_resp is not None:
                    last_resp.close()
                last_resp = resp
                continue

            # ── Success ─────────────────────────────────────────────
            report_key_result(idx, resp.status_code, resp.headers)
            if last_resp is not None:
                last_resp.close()
                last_resp = None

            out_headers = [
                (n, v) for n, v in resp.raw.headers.items()
                if n.lower() not in _HOP_BY_HOP
            ]
            # Pass resp as a default arg — NOT a closure capture —
            # so the binding survives loop exit / variable rebinding.
            return Response(_stream(resp), resp.status_code, out_headers)

        # ── All keys failed — forward the last upstream response ─────
        if last_resp is not None:
            out_headers = [
                (n, v) for n, v in last_resp.raw.headers.items()
                if n.lower() not in _HOP_BY_HOP
            ]
            _lr       = last_resp
            last_resp = None          # prevent double-close in finally
            return Response(_stream(_lr), _lr.status_code, out_headers)

        min_cd = min(
            (_key_state[i].cooldown_remaining() for i in range(len(_nvidia_keys))),
            default=10.0,
        )
        return Response(
            "All NVIDIA keys exhausted or on cooldown — retry shortly\n",
            503, {"Retry-After": str(int(min_cd) + 1)},
        )

    except Exception:
        log.exception("PROXY unhandled exception")
        return Response("Internal proxy error\n", 500)
    finally:
        # Guarantee cleanup of any response not consumed above
        if last_resp is not None:
            last_resp.close()


# ══════════════════════════════════════════════════════════════
#  GRACEFUL SHUTDOWN
# ══════════════════════════════════════════════════════════════

def _graceful_shutdown() -> None:
    """
    Idempotent shutdown sequence.
    Called by:
      • atexit (normal interpreter exit)
      • _signal_handler (SIGTERM / SIGINT) via a daemon thread
    The _shutdown_lock ensures the body runs at most once regardless
    of how many signals arrive concurrently.
    """
    global _shutdown_done
    with _shutdown_lock:
        if _shutdown_done:
            return
        _shutdown_done = True

    _shutdown_event.set()   # wake all blocking waits in background threads
    log.info("SHUTDOWN ▶ graceful shutdown initiated")

    _kill_hermes()
    log.info("SHUTDOWN ✓ Hermes process group stopped")

    if _sb_ok:
        log.info("SHUTDOWN ▶ uploading final state to Supabase...")
        try:
            sync_upload_all()
            log.info("SHUTDOWN ✓ state saved")
        except Exception as exc:
            log.error("SHUTDOWN ✗ state save failed: %s", exc)


def _signal_handler(signum: int, _frame) -> None:
    """
    Signal handlers must be fast and non-blocking.
    We schedule the real work on a daemon thread and arrange a
    forced exit after a generous timeout so the process never hangs.
    """
    name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
    log.info("SIGNAL %s received", name)

    # Run shutdown work off the signal-handling thread (avoids lock deadlocks)
    threading.Thread(target=_graceful_shutdown, daemon=True, name="shutdown").start()

    # Absolute safety net: if cleanup takes > 20s, force-exit.
    def _force_exit() -> None:
        time.sleep(20)
        log.warning("SHUTDOWN forced exit after 20s timeout")
        os._exit(0)

    threading.Thread(target=_force_exit, daemon=True, name="force-exit").start()


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT,  _signal_handler)
atexit.register(_graceful_shutdown)


# ══════════════════════════════════════════════════════════════
#  STARTUP SEQUENCE
# ══════════════════════════════════════════════════════════════

# ── 1. Verify Supabase (must complete before sync_download_all) ─
_sb_ok = _probe_supabase()

# ── 2. Restore persisted state ─────────────────────────────────
sync_download_all()

# ── 3. Write .env from environment vars (always overwrites) ────
def _write_dotenv() -> None:
    """
    Env vars are the authoritative source of secrets.
    Writing them to .env lets Hermes pick them up via python-dotenv.
    """
    lines: list[str] = []
    for var in (
        "TELEGRAM_BOT_TOKEN",
        *[f"NVIDIA_NIM_KEY_{i}" for i in range(1, 7)],
        "HONCHO_API_KEY",
        "EMAIL_ADDRESS", "EMAIL_PASSWORD",
        "EMAIL_IMAP_HOST", "EMAIL_IMAP_PORT",
        "EMAIL_SMTP_HOST", "EMAIL_SMTP_PORT",
        "GITHUB_TOKEN", "GH_TOKEN",
    ):
        v = os.environ.get(var, "")
        if v:
            lines.append(f"{var}={v}")
    lines.append("HERMES_AGENT_TIMEOUT=1800")
    (HERMES_HOME / ".env").write_text("\n".join(lines) + "\n")

    # Honcho sidecar config
    if os.environ.get("HONCHO_API_KEY"):
        honcho_dir = Path.home() / ".honcho"
        honcho_dir.mkdir(parents=True, exist_ok=True)
        cfg = honcho_dir / "config.json"
        if not cfg.exists():
            cfg.write_text(json.dumps({"enabled": True}))


_write_dotenv()


# ── 4. Init / patch config.yaml ────────────────────────────────
def _init_config() -> None:
    src = Path(__file__).parent / "config.yaml"
    dst = HERMES_HOME / "config.yaml"
    if src.exists() and not dst.exists():
        dst.write_text(src.read_text())
    if not dst.exists():
        return
    try:
        cfg = yaml.safe_load(dst.read_text()) or {}
        cfg.setdefault("network", {})["force_ipv4"] = True
        cfg.setdefault("agent",   {})["gateway_timeout"] = 1800
        tg_proxy = os.environ.get("TELEGRAM_API_URL", "").rstrip("/")
        if tg_proxy:
            if not tg_proxy.endswith("/bot"):
                tg_proxy += "/bot"
            cfg.setdefault("telegram", {}).setdefault("extra", {})["base_url"] = tg_proxy
        dst.write_text(yaml.dump(cfg, default_flow_style=False, allow_unicode=True))
    except Exception as exc:
        log.warning("BOOT config.yaml patch failed: %s", exc)


_init_config()


# ── 5. Build Hermes subprocess environment ──────────────────────
def _build_hermes_env() -> dict:
    env = os.environ.copy()
    env["HERMES_HOME"]      = str(HERMES_HOME)
    env["PYTHONUNBUFFERED"] = "1"
    proxy_base               = f"http://127.0.0.1:{PORT}/v1"
    env["NVIDIA_BASE_URL"]  = proxy_base
    env["NVIDIA_API_BASE"]  = proxy_base
    env["OPENAI_BASE_URL"]  = proxy_base
    if _nvidia_keys:
        env.setdefault("NVIDIA_API_KEY", _nvidia_keys[0])
    return env


_hermes_env = _build_hermes_env()


# ── 6. Boot summary ─────────────────────────────────────────────
def _boot_summary() -> None:
    files = [f for f in HERMES_HOME.rglob("*") if f.is_file()]
    dirs  = {
        f.relative_to(HERMES_HOME).parts[0]
        for f in files
        if len(f.relative_to(HERMES_HOME).parts) > 1
    }
    log.info("BOOT  NVIDIA keys:   %d loaded", len(_nvidia_keys))
    log.info("BOOT  Hermes home:   %s (%d files in %d dirs)", HERMES_HOME, len(files), len(dirs))
    if dirs:
        log.info("BOOT    ↳ dirs: %s", ", ".join(sorted(dirs)))
    log.info(
        "BOOT  Sync: %ds | Mem limit: %d MB | Watchdog: max %d restarts in %ds",
        SYNC_INTERVAL_SECS, MEM_LIMIT_MB, WATCHDOG_MAX_RESTARTS, WATCHDOG_WINDOW,
    )
    log.info(
        "BOOT  Supabase storage: %s | PostgreSQL: %s",
        "✅" if _sb_ok else "❌",
        "✅" if os.environ.get("DATABASE_URL") else "❌",
    )


_boot_summary()


# ── 7. Launch background threads ────────────────────────────────

threading.Thread(
    target=run_hermes_watchdog,
    args=(_hermes_env,),
    daemon=True,
    name="hermes-watchdog",
).start()

if _sb_ok:
    def _sync_loop() -> None:
        while not _shutdown_event.wait(timeout=SYNC_INTERVAL_SECS):
            try:
                sync_upload_changed()
            except Exception as exc:
                log.error("SYNC  periodic error: %s", exc)
    threading.Thread(target=_sync_loop, daemon=True, name="periodic-sync").start()
    log.info("BOOT  Periodic sync: every %ds", SYNC_INTERVAL_SECS)

threading.Thread(target=memory_watchdog, daemon=True, name="memory-monitor").start()


def _keep_alive_loop() -> None:
    host  = os.environ.get("SPACE_HOST", "")
    token = os.environ.get("HF_TOKEN", "")
    if not host or not token:
        log.info("KEEP  keep-alive disabled (SPACE_HOST or HF_TOKEN not set)")
        return
    url  = f"https://{host}/"
    hdrs = {"Authorization": f"Bearer {token}"}
    log.info("KEEP  → %s every %ds", url, KEEP_ALIVE_INTERVAL)
    while not _shutdown_event.wait(timeout=KEEP_ALIVE_INTERVAL):
        try:
            r = _http.get(url, headers=hdrs, timeout=15)
            if r.status_code not in (200, 302):
                log.warning("KEEP  HTTP %s", r.status_code)
        except Exception as exc:
            log.warning("KEEP  ping failed: %s", exc)


threading.Thread(target=_keep_alive_loop, daemon=True, name="keep-alive").start()

log.info("BOOT  ✅ All systems initialised — Hermes Agent is starting")

# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)