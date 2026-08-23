#!/usr/bin/env python3
"""本機儀表板：監看 dist/offline 抓取進度與 FinMind token 狀態。"""
from __future__ import annotations

import json
import os
import re
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
from urllib.parse import parse_qs, urlparse

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
HTML_FILE = SCRIPTS / "fetch_dashboard.html"

PORT = int(os.environ.get("FETCH_DASHBOARD_PORT", "8765"))
TOKEN_CACHE_SEC = 45
PROBE_URL = (
    "https://api.finmindtrade.com/api/v4/data"
    "?dataset=TaiwanStockPrice&data_id=2330&start_date=2026-08-21&end_date=2026-08-21"
)

_token_cache: dict = {"at": 0.0, "items": []}
_cache_lock = threading.Lock()
_inventory_cache: dict = {"at": 0.0, "payload": None}
INVENTORY_CACHE_SEC = 8

DATASET_ZH = {
    "TaiwanStockPrice": "股價",
    "TaiwanStockInstitutionalInvestorsBuySell": "法人買賣超",
    "TaiwanStockDayTrading": "當沖",
    "TaiwanStockMarginPurchaseShortSale": "融資券",
    "TaiwanFuturesDaily": "期貨日線",
    "TaiwanStockInfo": "股票清單",
}

SEG_ZH = {
    "price": "價量",
    "inst": "法人",
    "day": "當沖",
    "margin": "融資",
    "schema": "格式",
}


def asset_name_map() -> dict[str, str]:
    assets = read_json(DIST / "assets.json") or []
    if not isinstance(assets, list):
        assets = []
    out = {}
    for row in assets:
        if isinstance(row, dict) and row.get("id"):
            out[str(row["id"])] = str(row.get("name") or row["id"])
    return out


def humanize_log_line(line: str, names: dict[str, str] | None = None) -> str:
    """把常見英文 log 轉成可讀中文（保留原意）。"""
    s = line.strip()
    if not s:
        return ""
    names = names or {}

    m = re.search(r"=== (.+) (?:dashboard |fetch )?start ===", s)
    if m:
        return f"▶ 開始抓取（{m.group(1)}）"

    m = re.search(r"Loaded (\d+) FinMind tokens, workers=(\d+), hourly cap=(\d+)/token", s)
    if m:
        return f"已載入 {m.group(1)} 組 Token，並行 {m.group(2)} 路，每組每小時上限約 {m.group(3)} 次"

    m = re.search(r"Fetching data from (\S+) to (\S+)", s)
    if m:
        return f"抓取區間：{m.group(1)} ～ {m.group(2)}"

    if "Using cached universe" in s:
        m = re.search(r"\((\d+) assets\)", s)
        n = m.group(1) if m else "?"
        return f"使用快取標的清單（{n} 檔），略過 TaiwanStockInfo"

    m = re.search(r"Total target universe: (\d+) assets", s)
    if m:
        return f"目標宇宙：共 {m.group(1)} 個標的"

    m = re.search(r"Wrote dist stock_directory\.json \((\d+) entries\)", s)
    if m:
        return f"已寫入股票目錄（{m.group(1)} 筆）"

    m = re.search(r"Bundle seed: (\d+) files", s)
    if m:
        return f"已同步 App 內建 seed：{m.group(1)} 檔"

    m = re.search(r"Calendar (\S+) -> (\S+) \((\d+) sessions\)", s)
    if m:
        return f"交易日曆：{m.group(1)} → {m.group(2)}（{m.group(3)} 個交易日）"

    m = re.search(
        r"== round (\d+) complete (\d+)/(\d+) \(([\d.]+)%\) delisted/short (\d+) missing (\d+)",
        s,
    )
    if m:
        return (
            f"第 {m.group(1)} 輪驗證：完成 {m.group(2)}／{m.group(3)}（{m.group(4)}%），"
            f"下市／極短 {m.group(5)}，尚缺 {m.group(6)}"
        )

    m = re.search(r"\[(\d+)/(\d+)\] saved (\S+) segs=([^\s]+) days=(\d+)", s)
    if m:
        aid = m.group(3)
        name = names.get(aid, aid)
        segs = "、".join(SEG_ZH.get(x, x) for x in m.group(4).split(",") if x)
        return f"[{m.group(1)}/{m.group(2)}] 已寫入 {name}（{aid}）· {segs} · {m.group(5)} 日"

    m = re.search(r"\[(\d+)/(\d+)\] FAIL (\S+): (.+)", s)
    if m:
        aid = m.group(3)
        name = names.get(aid, aid)
        return f"[{m.group(1)}/{m.group(2)}] 失敗 {name}（{aid}）：{m.group(4)}"

    m = re.search(r"\[(Token-\d+) HTTP (\d+) on ([^/]+)/([^\]]+)\]", s)
    if m:
        ds = DATASET_ZH.get(m.group(3), m.group(3))
        aid = m.group(4)
        name = names.get(aid, aid) if aid not in ("None", "") else ""
        who = f"{name}（{aid}）" if name and aid != "None" else aid
        code = m.group(2)
        if code == "402":
            tip = "額度滿"
        elif code == "403":
            tip = "被拒絕／額度"
        elif code == "400":
            tip = "請求錯誤／Token 異常"
        else:
            tip = f"HTTP {code}"
        return f"[{m.group(1)}] {tip}（{code}）· {ds} · {who}"

    m = re.search(r"\[(Token-\d+) status (\d+) on ([^/]+)/([^\]]+)\]", s)
    if m:
        ds = DATASET_ZH.get(m.group(3), m.group(3))
        aid = m.group(4)
        name = names.get(aid, aid)
        return f"[{m.group(1)}] API 狀態 {m.group(2)} · {ds} · {name}（{aid}）"

    m = re.search(r"\[!\] (Token-\d+) quota 402/403 ×(\d+), cool (\d+)s", s)
    if m:
        return f"[{m.group(1)}] 額度滿，冷卻 {m.group(3)} 秒後換下一組（連續第 {m.group(2)} 次）"

    m = re.search(r"\[!\] (Token-\d+) cooling (\d+)s", s)
    if m:
        return f"[{m.group(1)}] 冷卻中 {m.group(2)} 秒"

    m = re.search(r"\[!\] (Token-\d+) invalid token", s)
    if m:
        return f"[{m.group(1)}] Token 無效，已暫停 24 小時"

    if "COMPLETE" in s and "assets verified" in s:
        return f"✅ 全部完成：{s}"

    if s.startswith("[!]") or s.startswith("["):
        return s
    return s


_TS_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s*(.*)$")
_DASH_TS_RE = re.compile(r"^===\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+")


def split_log_timestamp(line: str) -> tuple[str, str]:
    s = line.strip()
    m = _TS_RE.match(s)
    if m:
        return m.group(1), m.group(2)
    m = _DASH_TS_RE.match(s)
    if m:
        return m.group(1), s
    return "", s


def humanize_log_lines(lines: list[str]) -> list[dict]:
    names = asset_name_map()
    out: list[dict] = []
    last_ts = ""
    for line in lines:
        parts = re.split(r"(?=\[Token-)|(?=\[!\])|(?=\[\d+/\d+\])", line)
        for part in parts:
            part = part.strip()
            if not part:
                continue
            at, body = split_log_timestamp(part)
            if at:
                last_ts = at
            text = humanize_log_line(body or part, names)
            if not text:
                continue
            out.append({"at": at or last_ts, "text": text})
    return out[-80:]

def verify_metric_file(path: Path, expected_id: str) -> dict:
    """輕量驗證單檔：不展開每日明細。"""
    try:
        size = path.stat().st_size
    except OSError:
        return {"ok": False, "label": "無法讀取", "tone": "bad", "rows": 0, "first": None, "last": None, "bytes": 0}

    if size <= 2:
        return {"ok": False, "label": "空檔", "tone": "bad", "rows": 0, "first": None, "last": None, "bytes": size}

    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"ok": False, "label": "JSON 損壞", "tone": "bad", "rows": 0, "first": None, "last": None, "bytes": size}

    if not isinstance(rows, list) or not rows:
        return {"ok": False, "label": "無資料", "tone": "bad", "rows": 0, "first": None, "last": None, "bytes": size}

    first = rows[0] if isinstance(rows[0], dict) else {}
    last = rows[-1] if isinstance(rows[-1], dict) else {}
    first_date = first.get("date")
    last_date = last.get("date")
    closes = sum(1 for r in rows if isinstance(r, dict) and r.get("close") is not None)
    inst = any(isinstance(r, dict) and r.get("instNet") is not None for r in rows[-40:])
    mismatch = sum(1 for r in rows if isinstance(r, dict) and r.get("assetId") not in (None, expected_id))

    if mismatch:
        return {
            "ok": False,
            "label": "代號不符",
            "tone": "bad",
            "rows": len(rows),
            "first": first_date,
            "last": last_date,
            "bytes": size,
            "closes": closes,
        }
    if closes == 0:
        return {
            "ok": False,
            "label": "無收盤價",
            "tone": "bad",
            "rows": len(rows),
            "first": first_date,
            "last": last_date,
            "bytes": size,
            "closes": 0,
        }

    extras = []
    if inst:
        extras.append("法人")
    # day/margin sample
    if any(isinstance(r, dict) and r.get("dayVol") is not None for r in rows[-40:]):
        extras.append("當沖")
    if any(isinstance(r, dict) and r.get("marginDelta") is not None for r in rows[-40:]):
        extras.append("融資")

    if len(rows) >= 200 and closes >= max(1, int(len(rows) * 0.7)):
        label = "價量齊" + (("＋" + "／".join(extras)) if extras else "")
        tone = "ok"
        ok = True
    else:
        label = f"資料偏短（{len(rows)} 日）"
        tone = "warn"
        ok = True  # 下市／極短也算有檔

    return {
        "ok": ok,
        "label": label,
        "tone": tone,
        "rows": len(rows),
        "first": first_date,
        "last": last_date,
        "bytes": size,
        "closes": closes,
        "extras": extras,
    }


def build_inventory(limit: int = 80, offset: int = 0, q: str = "") -> dict:
    now = time.time()
    with _cache_lock:
        cached = _inventory_cache.get("payload")
        if cached and now - _inventory_cache["at"] < INVENTORY_CACHE_SEC and not q and offset == 0 and limit >= 80:
            # 僅無篩選的首頁快取
            items = cached["items"]
            summary = cached["summary"]
            page = items[offset: offset + limit]
            return {
                "generatedAt": datetime.now().isoformat(timespec="seconds"),
                "summary": summary,
                "total": len(items),
                "offset": offset,
                "limit": limit,
                "items": page,
                "cached": True,
            }

    names = asset_name_map()
    assets = read_json(DIST / "assets.json") or []
    type_map = {
        str(a["id"]): a.get("type", "stock")
        for a in assets
        if isinstance(a, dict) and a.get("id")
    } if isinstance(assets, list) else {}

    files = []
    if METRICS.exists():
        for path in METRICS.glob("*.json"):
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            files.append((mtime, path))
    files.sort(key=lambda x: x[0], reverse=True)  # 最近寫入在前

    items = []
    ok_n = warn_n = bad_n = 0
    for mtime, path in files:
        aid = path.stem
        name = names.get(aid, aid)
        if q and q.lower() not in aid.lower() and q.lower() not in name.lower():
            continue
        verified = verify_metric_file(path, aid)
        tone = verified["tone"]
        if tone == "ok":
            ok_n += 1
        elif tone == "warn":
            warn_n += 1
        else:
            bad_n += 1
        items.append({
            "id": aid,
            "name": name,
            "type": type_map.get(aid, "stock"),
            "mtime": datetime.fromtimestamp(mtime).isoformat(timespec="seconds"),
            **verified,
        })

    summary = {
        "files": len(files),
        "listed": len(items),
        "ok": ok_n,
        "warn": warn_n,
        "bad": bad_n,
    }

    if not q:
        with _cache_lock:
            _inventory_cache["at"] = now
            _inventory_cache["payload"] = {"items": items, "summary": summary}

    page = items[offset: offset + limit]
    return {
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
        "summary": summary,
        "total": len(items),
        "offset": offset,
        "limit": limit,
        "items": page,
        "cached": False,
    }


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


def clear_fetch_log() -> dict:
    """清空抓取 log（fetch_run.log）。執行中行程若以 append 寫入，後續仍可繼續寫。"""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    LOG_FILE.write_text(f"=== {stamp} log cleared ===\n", encoding="utf-8")
    return {"ok": True, "message": "已清除 log", "path": str(LOG_FILE.name)}


def start_fetch() -> dict:
    running, pid, _ = process_alive()
    if running:
        return {"ok": True, "alreadyRunning": True, "pid": pid, "message": "抓取已在執行中"}

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_handle = open(LOG_FILE, "a", encoding="utf-8")
    log_handle.write(f"\n=== {stamp} dashboard start ===\n")
    log_handle.flush()
    proc = subprocess.Popen(
        ["python3", "-u", str(FETCH_SCRIPT)],
        cwd=str(REPO_ROOT),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    # 子行程繼承 fd；父行程可關閉自己的副本
    try:
        log_handle.close()
    except Exception:
        pass
    PID_FILE.write_text(str(proc.pid))
    return {"ok": True, "alreadyRunning": False, "pid": proc.pid, "message": "已啟動背景抓取"}


def stop_fetch(force: bool = False) -> dict:
    pid = find_fetch_pid()
    if not pid:
        return {"ok": True, "stopped": False, "message": "沒有執行中的抓取行程"}
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        return {"ok": False, "message": str(exc)}
    for _ in range(10):
        time.sleep(0.1)
        if find_fetch_pid() is None:
            break
    else:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
        time.sleep(0.2)
    # 清掉殘留 pid 檔
    if find_fetch_pid() is None and PID_FILE.exists():
        try:
            PID_FILE.unlink()
        except OSError:
            pass
    return {"ok": True, "stopped": True, "pid": pid, "message": "已送出停止訊號"}


def kick_fetch() -> dict:
    """手動測試：停掉舊行程並立刻重啟，清掉記憶體內冷卻，立刻再打 API。"""
    stopped = stop_fetch(force=True)
    # 再保險掃一次
    leftover = find_fetch_pid()
    if leftover:
        try:
            os.kill(leftover, signal.SIGKILL)
        except OSError:
            pass
        time.sleep(0.2)
    started = start_fetch()
    if started.get("alreadyRunning"):
        # 極少數情況：stop 失敗仍顯示 running → 強制再殺一次
        stop_fetch(force=True)
        time.sleep(0.3)
        started = start_fetch()
    return {
        "ok": bool(started.get("ok")) and not started.get("alreadyRunning", False) or bool(started.get("pid")),
        "stopped": stopped.get("stopped", False),
        "oldPid": stopped.get("pid"),
        "pid": started.get("pid"),
        "message": (
            f"已強制續抓（清冷卻）。"
            f"{'停掉 PID ' + str(stopped['pid']) + ' → ' if stopped.get('stopped') else ''}"
            f"新 PID {started.get('pid')}。若仍 402/403 會再進短冷卻。"
        ),
    }


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
            out.append({"name": item.get("name") or "token", "token": str(item["token"]).strip()})
        elif isinstance(item, str) and item.strip():
            out.append({"name": "token", "token": item.strip()})
    return out


def save_token_entries(entries: list[dict]) -> dict:
    """寫入 finmind_tokens.local.json（僅本機儀表板）。"""
    cleaned = []
    seen = set()
    for item in entries:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip() or "token"
        token = str(item.get("token") or "").strip()
        if not token:
            continue
        if token in seen:
            continue
        seen.add(token)
        cleaned.append({"name": name, "token": token})

    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(
        json.dumps(cleaned, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with _cache_lock:
        _token_cache["at"] = 0.0
        _token_cache["items"] = []
    return {
        "ok": True,
        "count": len(cleaned),
        "path": str(TOKEN_FILE.name),
        "message": f"已儲存 {len(cleaned)} 組 Token 到 {TOKEN_FILE.name}。抓取行程需「強制續抓」才會載入新 Token。",
        "entries": [
            {"index": i, "name": e["name"], "token": e["token"], "tail": e["token"][-8:]}
            for i, e in enumerate(cleaned)
        ],
    }


def list_tokens_for_edit() -> dict:
    entries = load_token_entries()
    return {
        "ok": True,
        "path": str(TOKEN_FILE.name),
        "count": len(entries),
        "entries": [
            {"index": i, "name": e["name"], "token": e["token"], "tail": e["token"][-8:]}
            for i, e in enumerate(entries)
        ],
    }


def _read_json_body(handler: BaseHTTPRequestHandler) -> dict | list | None:
    length = int(handler.headers.get("Content-Length") or 0)
    if length <= 0:
        return None
    raw = handler.rfile.read(length)
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


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
            cached = [dict(x) for x in _token_cache["items"]]
            return apply_log_quota_overlay(cached)

    entries = load_token_entries()
    items: list[dict] = []
    if entries:
        with ThreadPoolExecutor(max_workers=min(10, len(entries))) as pool:
            futures = [pool.submit(probe_token, e) for e in entries]
            # 保持與 finmind_tokens.local.json / Token-N 相同順序（勿依 name 排序）
            items = [fut.result() for fut in futures]

    with _cache_lock:
        _token_cache["at"] = now
        _token_cache["items"] = [dict(x) for x in items]
    return apply_log_quota_overlay([dict(x) for x in items])


def recent_log_quota_hits(max_lines: int = 400) -> dict[int, int]:
    """
    從最近 log（自上次 Loaded tokens 起）解析 Token-N → 402/403。
    抓取行程實際撞到的額度，比單獨 probe 快取更準。
    """
    lines = tail_lines(LOG_FILE, max_lines)
    if not lines:
        return {}
    start = 0
    for i, line in enumerate(lines):
        if "Loaded " in line and "FinMind tokens" in line:
            start = i
    hits: dict[int, int] = {}
    for line in lines[start:]:
        for m in re.finditer(r"Token-(\d+)\s+HTTP\s+(402|403)\b", line):
            hits[int(m.group(1))] = int(m.group(2))
        for m in re.finditer(r"Token-(\d+)\s+status\s+(402|403)\b", line):
            hits[int(m.group(1))] = int(m.group(2))
        for m in re.finditer(r"Token-(\d+)\s+quota\s+402/403", line):
            hits.setdefault(int(m.group(1)), 402)
    return hits


def apply_log_quota_overlay(items: list[dict]) -> list[dict]:
    """若抓取 log 已出現 402/403，覆蓋仍顯示「可用」的 probe 快取。"""
    if not items:
        return items
    hits = recent_log_quota_hits()
    if not hits:
        return items
    for idx, code in hits.items():
        if idx < 0 or idx >= len(items):
            continue
        cur = items[idx]
        if cur.get("category") == "quota" and cur.get("code") in (402, 403):
            continue
        label, tone, category = classify_token_status(code, "reach the upper limit")
        items[idx] = {
            **cur,
            "status": code,
            "code": code,
            "category": category,
            "label": label,
            "tone": tone,
            "msg": format_token_msg(code, "reach the upper limit") + "（依抓取 log）",
            "fromLog": True,
        }
    return items


def build_summary(refresh_tokens: bool = False) -> dict:
    running, pid, proc_note = process_alive()
    status = read_json(STATUS_FILE) or {}
    manifest = read_json(MANIFEST_FILE) or {}
    entries = manifest.get("entries") if isinstance(manifest, dict) else None
    tokens = probe_tokens_cached(force=refresh_tokens)
    log_tail = humanize_log_lines(tail_lines(LOG_FILE, 80))
    inventory = build_inventory(limit=60, offset=0)

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
        "inventory": inventory,
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
        if path == "/api/inventory":
            qs = parse_qs(urlparse(self.path).query)
            try:
                limit = min(int((qs.get("limit") or ["80"])[0]), 500)
            except ValueError:
                limit = 80
            try:
                offset = max(int((qs.get("offset") or ["0"])[0]), 0)
            except ValueError:
                offset = 0
            q = (qs.get("q") or [""])[0].strip()
            payload = build_inventory(limit=limit, offset=offset, q=q)
            self._send(200, json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        if path == "/api/tokens":
            payload = list_tokens_for_edit()
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
        if path == "/api/fetch/kick":
            payload = kick_fetch()
            self._send(200, json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        if path == "/api/log/clear":
            payload = clear_fetch_log()
            self._send(200, json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        if path == "/api/tokens/save":
            body = _read_json_body(self)
            if not isinstance(body, dict) or not isinstance(body.get("entries"), list):
                self._send(
                    400,
                    json.dumps({"ok": False, "message": "請傳送 { entries: [{name, token}, ...] }"}, ensure_ascii=False).encode(),
                    "application/json; charset=utf-8",
                )
                return
            payload = save_token_entries(body["entries"])
            kick = bool(body.get("kick"))
            if kick:
                kick_result = kick_fetch()
                payload["kick"] = kick_result
                payload["message"] = payload["message"] + " " + kick_result.get("message", "")
            self._send(200, json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        self._send(404, b"not found", "text/plain; charset=utf-8")


def main():
    if not HTML_FILE.exists():
        raise SystemExit(f"Missing {HTML_FILE}")

    ThreadingHTTPServer.allow_reuse_address = True
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    url = f"http://127.0.0.1:{PORT}/"
    print(f"MT 離線抓取儀表板 → {url}", flush=True)
    print("Ctrl+C 停止（不影響背景 fetch）", flush=True)

    def stop(*_):
        print("\n儀表板關閉中…", flush=True)
        server.shutdown()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
