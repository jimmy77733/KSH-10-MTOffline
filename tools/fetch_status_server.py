#!/usr/bin/env python3
"""本機儀表板：監看 dist/offline 抓取進度與 FinMind token 狀態。"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

TOOLS = Path(__file__).resolve().parent
REPO_ROOT = Path(os.environ.get("MT_REPO_ROOT", TOOLS.parent))
if os.environ.get("MT_OFFLINE_ROOT"):
    DIST = Path(os.environ["MT_OFFLINE_ROOT"]).resolve()
elif (REPO_ROOT / "StockCalendar").is_dir():
    DIST = REPO_ROOT / "dist" / "offline"
else:
    DIST = REPO_ROOT
SCRIPTS = TOOLS
METRICS = DIST / "metrics"
STATUS_FILE = DIST / "status.json"
MANIFEST_FILE = DIST / "manifest.json"
LOG_FILE = SCRIPTS / "fetch_run.log"
FETCH_SCRIPT = SCRIPTS / "fetch_historical_data.py"
PID_FILE = SCRIPTS / "fetch.pid"
TOKEN_FILE = SCRIPTS / "finmind_tokens.local.json"
HTML_FILE = TOOLS / "fetch_dashboard.html"

PORT = int(os.environ.get("FETCH_DASHBOARD_PORT", "8765"))
TOKEN_CACHE_SEC = 45
PROBE_URL = (
    "https://api.finmindtrade.com/api/v4/data"
    "?dataset=TaiwanStockPrice&data_id=2330&start_date=2026-08-21&end_date=2026-08-21"
)

_token_cache: dict = {"at": 0.0, "items": []}
_cache_lock = threading.Lock()


def read_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def tail_lines(path: Path, n: int = 50) -> list[str]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return lines[-n:]
    except Exception:
        return []


def find_fetch_pid() -> int | None:
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", "fetch_historical_data.py"],
            text=True,
        )
        pids = [int(line) for line in out.splitlines() if line.strip().isdigit()]
        return pids[0] if pids else None
    except (subprocess.CalledProcessError, FileNotFoundError, ValueError):
        return None


def process_alive() -> tuple[bool, int | None, str | None]:
    pid = find_fetch_pid()
    if pid:
        PID_FILE.write_text(str(pid))
        return True, pid, None
    stale = None
    if PID_FILE.exists():
        try:
            stale = int(PID_FILE.read_text().strip())
        except ValueError:
            stale = None
    if stale:
        return False, stale, "stale_pid"
    return False, None, None


def start_fetch() -> dict:
    running, pid, _ = process_alive()
    if running:
        return {"ok": True, "alreadyRunning": True, "pid": pid, "message": "抓取已在執行中"}

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as log_handle:
        log_handle.write(f"\n=== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} dashboard start ===\n")
        log_handle.flush()
        proc = subprocess.Popen(
            ["python3", "-u", str(FETCH_SCRIPT)],
            cwd=str(REPO_ROOT),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    PID_FILE.write_text(str(proc.pid))
    return {"ok": True, "alreadyRunning": False, "pid": proc.pid, "message": "已啟動背景抓取"}


def stop_fetch() -> dict:
    pid = find_fetch_pid()
    if not pid:
        return {"ok": True, "stopped": False, "message": "沒有執行中的抓取行程"}
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        return {"ok": False, "message": str(exc)}
    return {"ok": True, "stopped": True, "pid": pid, "message": "已送出停止訊號"}


def _kill0(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def metrics_stats() -> dict:
    if not METRICS.exists():
        return {"count": 0, "bytes": 0}
    count = 0
    total = 0
    for path in METRICS.glob("*.json"):
        try:
            total += path.stat().st_size
            count += 1
        except OSError:
            pass
    return {"count": count, "bytes": total}


def load_token_entries() -> list[dict]:
    raw = read_json(TOKEN_FILE)
    if not isinstance(raw, list):
        return []
    out = []
    for item in raw:
        if isinstance(item, dict) and item.get("token"):
            out.append({"name": item.get("name") or "token", "token": item["token"]})
        elif isinstance(item, str) and item.strip():
            out.append({"name": "token", "token": item.strip()})
    return out


def probe_token(entry: dict) -> dict:
    name = entry["name"]
    token = entry["token"]
    url = PROBE_URL + "&token=" + urllib.parse.quote(token)
    started = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "StockCalendar-dashboard/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = json.loads(resp.read().decode())
        status = body.get("status", 0)
        msg = body.get("msg", "")
    except urllib.error.HTTPError as exc:
        try:
            body = json.loads(exc.read().decode() or "{}")
            status = body.get("status", exc.code)
            msg = body.get("msg", str(exc))
        except Exception:
            status = exc.code
            msg = str(exc)
    except Exception as exc:
        status = "err"
        msg = str(exc)

    label, tone, category = classify_token_status(status, msg)
    code = _status_code(status)
    return {
        "name": name,
        "status": status,
        "code": code,
        "category": category,
        "label": label,
        "tone": tone,
        "msg": format_token_msg(code, msg),
        "rawMsg": msg or "",
        "ms": int((time.time() - started) * 1000),
        "tail": token[-8:],
    }


def _status_code(status) -> int | None:
    try:
        return int(status)
    except (TypeError, ValueError):
        return None


def format_token_msg(code: int | None, msg: str) -> str:
    msg_l = (msg or "").lower()
    if code == 200:
        return "可正常請求"
    if code == 402 or "upper limit" in msg_l or "reach the upper limit" in msg_l:
        return "每小時 600 次已用完，等整點重置"
    if code == 403:
        return "請求被拒／額度或權限限制"
    if code == 401:
        return "未授權或 token 過期"
    if code == 429:
        return "請求過於頻繁，稍後再試"
    if code == 400 and "illegal" in msg_l:
        return "Token 無效，請到 FinMind 重新產生"
    if msg:
        return msg
    if code is not None:
        return f"HTTP {code}"
    return "未知錯誤"


def classify_token_status(status, msg: str) -> tuple[str, str, str]:
    """回傳 (label, tone, category)。category: ok | quota | bad | warn"""
    msg_l = (msg or "").lower()
    code = _status_code(status)
    if code == 200:
        return "可用", "ok", "ok"
    if code == 402 or "upper limit" in msg_l or "reach the upper limit" in msg_l:
        return "402 額度滿", "quota", "quota"
    if code == 403:
        return "403 額度滿", "quota", "quota"
    if code == 401:
        return "401 未授權", "warn", "warn"
    if code == 400 and "illegal" in msg_l:
        return "400 無效", "bad", "bad"
    if code == 429:
        return "429 限流", "warn", "warn"
    if code is not None:
        return f"{code} 異常", "warn", "warn"
    return "未知", "warn", "warn"


def probe_tokens_cached(force: bool = False) -> list[dict]:
    now = time.time()
    with _cache_lock:
        if not force and _token_cache["items"] and now - _token_cache["at"] < TOKEN_CACHE_SEC:
            return _token_cache["items"]

    entries = load_token_entries()
    items = []
    if entries:
        with ThreadPoolExecutor(max_workers=min(10, len(entries))) as pool:
            futures = [pool.submit(probe_token, e) for e in entries]
            for fut in as_completed(futures):
                items.append(fut.result())
        items.sort(key=lambda x: x["name"])

    with _cache_lock:
        _token_cache["at"] = now
        _token_cache["items"] = items
    return items


def build_summary(refresh_tokens: bool = False) -> dict:
    running, pid, proc_note = process_alive()
    status = read_json(STATUS_FILE) or {}
    manifest = read_json(MANIFEST_FILE) or {}
    entries = manifest.get("entries") if isinstance(manifest, dict) else None
    tokens = probe_tokens_cached(force=refresh_tokens)
    log_tail = tail_lines(LOG_FILE, 60)

    complete = status.get("complete") or 0
    universe = status.get("universe") or 0
    pct = status.get("coveragePercent")
    if pct is None and universe:
        pct = round(100.0 * complete / universe, 2)

    token_summary = {
        "total": len(tokens),
        "ok": sum(1 for t in tokens if t.get("category") == "ok"),
        "quota": sum(1 for t in tokens if t.get("category") == "quota"),
        "quota402": sum(1 for t in tokens if t.get("code") == 402),
        "quota403": sum(1 for t in tokens if t.get("code") == 403),
        "bad": sum(1 for t in tokens if t.get("category") == "bad"),
        "warn": sum(1 for t in tokens if t.get("category") == "warn"),
    }

    return {
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
        "process": {"running": running, "pid": pid, "note": proc_note},
        "status": status,
        "progress": {
            "complete": complete,
            "universe": universe,
            "missing": status.get("missing"),
            "delistedOrShort": status.get("delistedOrShort"),
            "round": status.get("round"),
            "coveragePercent": pct,
            "calendarLast": status.get("calendarLast"),
            "updatedAt": status.get("updatedAt"),
        },
        "metrics": metrics_stats(),
        "manifest": {
            "exists": MANIFEST_FILE.exists(),
            "entries": len(entries) if isinstance(entries, list) else 0,
            "version": manifest.get("version") if isinstance(manifest, dict) else None,
        },
        "tokens": tokens,
        "tokenSummary": token_summary,
        "logTail": log_tail,
        "paths": {
            "log": str(LOG_FILE.relative_to(REPO_ROOT)),
            "status": str(STATUS_FILE.relative_to(REPO_ROOT)),
            "metrics": str(METRICS.relative_to(REPO_ROOT)),
        },
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def _send(self, code: int, body: bytes, content_type: str):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            if not HTML_FILE.exists():
                self._send(404, b"missing fetch_dashboard.html", "text/plain; charset=utf-8")
                return
            self._send(200, HTML_FILE.read_bytes(), "text/html; charset=utf-8")
            return
        if path == "/api/summary":
            refresh = "refresh=1" in (self.path.split("?", 1)[1] if "?" in self.path else "")
            payload = build_summary(refresh_tokens=refresh)
            self._send(200, json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        self._send(404, b"not found", "text/plain; charset=utf-8")

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path == "/api/fetch/start":
            payload = start_fetch()
            self._send(200, json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        if path == "/api/fetch/stop":
            payload = stop_fetch()
            self._send(200, json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        self._send(404, b"not found", "text/plain; charset=utf-8")


def main():
    if not HTML_FILE.exists():
        raise SystemExit(f"Missing {HTML_FILE}")

    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    url = f"http://127.0.0.1:{PORT}/"
    print(f"MT 離線抓取儀表板 → {url}")
    print("Ctrl+C 停止（不影響背景 fetch）")

    def stop(*_):
        server.shutdown()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    server.serve_forever()


if __name__ == "__main__":
    main()
