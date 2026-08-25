import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from hashlib import sha256
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


TAIPEI_TZ = ZoneInfo("Asia/Taipei")


def taipei_now():
    """回傳明確使用台北時區的目前時間。"""
    return datetime.now(TAIPEI_TZ)


class _TimestampStdout:
    """為每行 log 加上 [YYYY-MM-DD HH:MM:SS] 時間軸。"""

    _HAS_TS = re.compile(r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]")

    def __init__(self, stream):
        self._stream = stream
        self._buf = ""

    def write(self, s):
        if not isinstance(s, str):
            s = str(s)
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if self._HAS_TS.match(line) or line.startswith("==="):
                self._stream.write(line + "\n")
            else:
                ts = taipei_now().strftime("%Y-%m-%d %H:%M:%S")
                self._stream.write(f"[{ts}] {line}\n")

    def flush(self):
        if self._buf:
            if self._HAS_TS.match(self._buf) or self._buf.startswith("==="):
                self._stream.write(self._buf)
            else:
                ts = taipei_now().strftime("%Y-%m-%d %H:%M:%S")
                self._stream.write(f"[{ts}] {self._buf}")
            self._buf = ""
        self._stream.flush()

    def fileno(self):
        return self._stream.fileno()

    def isatty(self):
        return self._stream.isatty()


if not isinstance(sys.stdout, _TimestampStdout):
    sys.stdout = _TimestampStdout(sys.stdout)
if not isinstance(sys.stderr, _TimestampStdout):
    sys.stderr = _TimestampStdout(sys.stderr)

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TOOLS_DIR)

# 資料輸出根目錄：StockCalendar 預設 dist/offline；公開 repo clone 設 MT_OFFLINE_ROOT=. 
_default_dist = os.path.join(REPO_ROOT, "dist", "offline")
if os.environ.get("MT_OFFLINE_ROOT"):
    DIST_DIR = os.path.abspath(os.environ["MT_OFFLINE_ROOT"])
elif os.path.isdir(os.path.join(REPO_ROOT, "StockCalendar")):
    DIST_DIR = _default_dist
else:
    DIST_DIR = REPO_ROOT

METRICS_DIR = os.path.join(DIST_DIR, "metrics")
STOCK_INFO_CACHE = os.path.join(DIST_DIR, "_taiwan_stock_info.json")
NEGATIVE_CACHE_PATH = os.path.join(DIST_DIR, "dataset_negative_cache.json")

# 可選：同步熱門 seed 進 iOS App（僅 StockCalendar 專案）
APP_SEED_DIR = os.environ.get("MT_APP_SEED_DIR")
if not APP_SEED_DIR and os.path.isdir(os.path.join(REPO_ROOT, "StockCalendar", "Resources", "PrebundledData")):
    APP_SEED_DIR = os.path.join(REPO_ROOT, "StockCalendar", "Resources", "PrebundledData")
RESOURCES_DIR = APP_SEED_DIR or os.path.join(DIST_DIR, "_scratch")
SEED_METRICS_DIR = os.path.join(RESOURCES_DIR, "metrics") if APP_SEED_DIR else None

os.makedirs(DIST_DIR, exist_ok=True)
os.makedirs(METRICS_DIR, exist_ok=True)
if SEED_METRICS_DIR:
    os.makedirs(SEED_METRICS_DIR, exist_ok=True)


def atomic_json_dump(path, payload, **kwargs):
    """以同目錄暫存檔原子寫入 JSON，避免中斷時留下截斷檔案。"""
    directory = os.path.dirname(path) or "."
    fd, temp_path = tempfile.mkstemp(
        prefix=f".{os.path.basename(path)}.",
        suffix=".tmp",
        dir=directory,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, **kwargs)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except Exception:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
        raise

# 進 IPA 的子集（非下載範圍）。完整庫是過濾後的全部台股標的。
BUNDLE_SEED_IDS = {
    "2330", "2317", "2454", "2303", "2881", "2882", "2308", "2382", "2412", "2891",
    "2886", "3711", "2603", "2609", "2615", "3034", "2379", "2345", "3231", "6669",
    "0050", "0056", "00878", "00919", "00929", "006208", "00679B", "00713",
    "1101", "1216", "1301", "2002", "2207",
    "TAIEX", "TPEx", "TX",
}

SPARK_COUNT = 20
MAX_WORKERS = 10
TOKEN_HOURLY_LIMIT = 580
TOKEN_COOLDOWN_SEC = 60
# 402/403：硬上限，絕不再用「等到下一整點」（曾誤判睡近 1 小時）
QUOTA_COOLDOWN_BASE_SEC = 60
QUOTA_COOLDOWN_MAX_SEC = 120
STALL_ROUNDS_LIMIT = 8
TOKEN_FILE = os.path.join(TOOLS_DIR, "finmind_tokens.local.json")
TOKEN_FILE_TXT = os.path.join(TOOLS_DIR, "finmind_tokens.local.txt")
RECENT_EXTRA_WINDOW = 40
DELISTED_MAX_RATIO = 0.20


def load_tokens():
    """從 gitignore 的本機檔讀 token；勿把 token 寫進版控。"""
    tokens = []
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, str) and item.strip():
                    tokens.append(item.strip())
                elif isinstance(item, dict) and item.get("token"):
                    tokens.append(str(item["token"]).strip())
    if not tokens and os.path.exists(TOKEN_FILE_TXT):
        with open(TOKEN_FILE_TXT, "r", encoding="utf-8") as handle:
            tokens = [line.strip() for line in handle if line.strip() and not line.startswith("#")]
    env = os.environ.get("FINMIND_TOKENS", "").strip()
    if env:
        tokens.extend(t.strip() for t in env.split(",") if t.strip())
    seen = set()
    out = []
    for tok in tokens:
        if tok not in seen:
            seen.add(tok)
            out.append(tok)
    if not out:
        raise SystemExit(
            f"No FinMind tokens. Create {TOKEN_FILE} (gitignored) or set FINMIND_TOKENS."
        )
    return out


TOKENS = load_tokens()
MAX_WORKERS = min(MAX_WORKERS, max(len(TOKENS), 1))

end_date = taipei_now()
start_date = end_date - timedelta(days=1825)
START_STR = start_date.strftime("%Y-%m-%d")
END_STR = end_date.strftime("%Y-%m-%d")

metrics_lock = threading.Lock()
negative_cache_lock = threading.Lock()
RUN_ID = taipei_now().isoformat(timespec="seconds")
MARKET_TYPES = {}


def load_negative_cache():
    if not os.path.exists(NEGATIVE_CACHE_PATH):
        return {}
    try:
        with open(NEGATIVE_CACHE_PATH, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        print(f"Error loading dataset negative cache: {exc}")
        return {}


DATASET_NEGATIVE_CACHE = load_negative_cache()


def load_market_types(directory=None):
    """讀取既有 stock_directory.json，並以本次目錄資料補齊市場別。"""
    rows = []
    path = os.path.join(DIST_DIR, "stock_directory.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, list):
                rows.extend(payload)
        except Exception as exc:
            print(f"Error loading stock directory market types: {exc}")
    if directory:
        rows.extend(directory)
    return {
        row["stockId"]: row.get("type", "")
        for row in rows
        if isinstance(row, dict) and row.get("stockId")
    }


def dataset_is_skipped(asset_id, segment):
    with negative_cache_lock:
        entry = DATASET_NEGATIVE_CACHE.get(asset_id, {}).get(segment, {})
        return (
            entry.get("emptyRuns", 0) >= 3
            or entry.get("lastRun") == RUN_ID
        )


def record_dataset_result(asset_id, segment, rows):
    """記錄連續三次執行皆無資料的 dataset，避免無限重抓。"""
    with negative_cache_lock:
        asset_cache = DATASET_NEGATIVE_CACHE.setdefault(asset_id, {})
        if rows:
            if segment in asset_cache:
                del asset_cache[segment]
            if not asset_cache:
                DATASET_NEGATIVE_CACHE.pop(asset_id, None)
        else:
            entry = asset_cache.setdefault(segment, {})
            if entry.get("lastRun") != RUN_ID:
                entry["emptyRuns"] = int(entry.get("emptyRuns", 0)) + 1
                entry["lastRun"] = RUN_ID
            if entry.get("emptyRuns", 0) >= 3:
                entry["notApplicable"] = True
        atomic_json_dump(
            NEGATIVE_CACHE_PATH,
            DATASET_NEGATIVE_CACHE,
            ensure_ascii=False,
            indent=2,
        )


class FetchFailed(Exception):
    pass


class TokenPool:
    def __init__(self, tokens):
        self.tokens = list(tokens)
        self.cooldown_until = [0.0] * len(tokens)
        self.lock = threading.Lock()
        self.cursor = 0
        self.hard_fail_streak = [0] * len(tokens)
        self.quota_streak = [0] * len(tokens)
        self.hourly_hits = [[] for _ in tokens]
        self.names = [f"Token-{i}" for i in range(len(tokens))]

    def _prune_hourly(self, idx, now):
        cutoff = now - 3600
        self.hourly_hits[idx] = [t for t in self.hourly_hits[idx] if t > cutoff]

    def _hourly_count(self, idx, now):
        self._prune_hourly(idx, now)
        return len(self.hourly_hits[idx])

    def acquire(self, wait_timeout=900):
        deadline = time.time() + wait_timeout
        while True:
            with self.lock:
                now = time.time()
                n = len(self.tokens)
                candidates = []
                best_wait = None
                for i in range(n):
                    idx = (self.cursor + i) % n
                    ready_at = self.cooldown_until[idx]
                    if now < ready_at:
                        wait = ready_at - now
                        best_wait = wait if best_wait is None else min(best_wait, wait)
                        continue
                    if self._hourly_count(idx, now) >= TOKEN_HOURLY_LIMIT:
                        # 本地預估達上限：短冷卻，讓其他組先跑
                        until = now + QUOTA_COOLDOWN_BASE_SEC
                        if until > self.cooldown_until[idx]:
                            self.cooldown_until[idx] = until
                        best_wait = QUOTA_COOLDOWN_BASE_SEC if best_wait is None else min(best_wait, QUOTA_COOLDOWN_BASE_SEC)
                        continue
                    usage = self._hourly_count(idx, now)
                    candidates.append((usage, self.quota_streak[idx], idx))
                if candidates:
                    # 優先：用量少、近期少被 402
                    candidates.sort(key=lambda item: (item[0], item[1], item[2]))
                    idx = candidates[0][2]
                    self.cursor = (idx + 1) % n
                    return idx, self.tokens[idx]
            if time.time() >= deadline:
                raise FetchFailed("all tokens still cooling or hourly limit reached")
            # 全部冷卻時最多睡 5 秒再掃一次，避免誤判長睡
            time.sleep(max(1.0, min(best_wait or 5.0, 5.0)))

    def block(self, idx, seconds=TOKEN_COOLDOWN_SEC):
        """一般冷卻；秒數硬上限 10 分鐘，防止誤設成近 1 小時。"""
        seconds = min(float(seconds), 600.0)
        with self.lock:
            until = time.time() + seconds
            if until > self.cooldown_until[idx]:
                self.cooldown_until[idx] = until
            print(f"[!] {self.names[idx]} cooling {seconds:.0f}s")

    def block_quota(self, idx):
        """
        FinMind 402/403：只冷卻這一組並立刻換下一組。
        遞增 60→90→120 秒，硬上限 120 秒。絕不用「等到下一整點」。
        """
        with self.lock:
            self.quota_streak[idx] += 1
            streak = self.quota_streak[idx]
            wait = min(QUOTA_COOLDOWN_BASE_SEC + (streak - 1) * 30, QUOTA_COOLDOWN_MAX_SEC)
            until = time.time() + wait
            if until > self.cooldown_until[idx]:
                self.cooldown_until[idx] = until
            print(f"[!] {self.names[idx]} quota 402/403 ×{streak}, cool {wait:.0f}s → rotate")

    def note_ok(self, idx):
        with self.lock:
            self.hard_fail_streak[idx] = 0
            self.quota_streak[idx] = 0
            self.hourly_hits[idx].append(time.time())

    def note_hard_fail(self, idx, illegal=False):
        with self.lock:
            self.hard_fail_streak[idx] += 1
            streak = self.hard_fail_streak[idx]
        if illegal or streak >= 3:
            self.block(idx, 24 * 3600)
            print(f"[!] {self.names[idx]} invalid token, paused 24h")
            return
        if streak >= 2:
            self.block(idx, 300)


POOL = TokenPool(TOKENS)
print(f"Loaded {len(TOKENS)} FinMind tokens, workers={MAX_WORKERS}, hourly cap={TOKEN_HOURLY_LIMIT}/token")


def is_quota_status(status):
    try:
        code = int(status)
    except (TypeError, ValueError):
        return False
    return code in (401, 402, 403)


def fetch_data(dataset, data_id, start=None, end=None):
    """成功回傳 list（可為空）。額度問題會換 token 重試，不會假裝成功。"""
    parts = [f"dataset={urllib.parse.quote(dataset)}"]
    if start:
        parts.append(f"start_date={start}")
    if end:
        parts.append(f"end_date={end}")
    if data_id:
        parts.append(f"data_id={urllib.parse.quote(data_id)}")
    url = "https://api.finmindtrade.com/api/v4/data?" + "&".join(parts)

    last_error = None
    deadline = time.time() + 12 * 3600
    while time.time() < deadline:
        idx, token = POOL.acquire(wait_timeout=7200)
        req_url = url + f"&token={urllib.parse.quote(token)}"
        try:
            req = urllib.request.Request(req_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=45) as response:
                payload = json.loads(response.read().decode("utf-8"))
            status = payload.get("status")
            if status == 200:
                POOL.note_ok(idx)
                return payload.get("data") or []
            if is_quota_status(status):
                print(f"[{POOL.names[idx]} status {status} on {dataset}/{data_id}]")
                POOL.block_quota(idx)
                last_error = f"status {status}"
                continue
            print(f"[!] API {status} {payload.get('msg')} for {dataset}/{data_id}")
            last_error = f"status {status}"
            continue
        except urllib.error.HTTPError as exc:
            body_msg = ""
            try:
                body_msg = json.loads(exc.read().decode() or "{}").get("msg", "")
            except Exception:
                body_msg = ""
            illegal = exc.code == 400 and "illegal" in body_msg.lower()
            if exc.code in (400, 404, 422):
                print(f"[{POOL.names[idx]} HTTP {exc.code} on {dataset}/{data_id}, retry other token]")
                POOL.block(idx, 15)
                POOL.note_hard_fail(idx, illegal=illegal)
                last_error = f"HTTP {exc.code}"
                continue
            if exc.code in (401, 402, 403, 429):
                print(f"[{POOL.names[idx]} HTTP {exc.code} on {dataset}/{data_id}]")
                if exc.code in (402, 403):
                    POOL.block_quota(idx)
                elif exc.code == 429:
                    POOL.block(idx, 90)
                else:
                    POOL.block(idx, 120)
                last_error = f"HTTP {exc.code}"
                continue
            last_error = f"HTTP {exc.code}"
            print(f"HTTP {exc.code} on {dataset}/{data_id}")
            time.sleep(3)
        except FetchFailed as exc:
            last_error = str(exc)
            time.sleep(5)
        except Exception as exc:
            last_error = str(exc)
            print(f"Exception on {dataset}/{data_id}: {exc}")
            time.sleep(3)
    raise FetchFailed(f"{dataset}/{data_id}: {last_error}")


def float_val(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def metrics_path(asset_id):
    return os.path.join(METRICS_DIR, f"{asset_id}.json")


def rows_for(asset_id, dates):
    rows = []
    for day, vals in dates.items():
        row = {"assetId": asset_id, "date": day}
        row.update(vals)
        rows.append(row)
    rows.sort(key=lambda item: item["date"])
    return rows


def save_asset_metrics(asset_id, dates):
    if not dates:
        return
    atomic_json_dump(
        metrics_path(asset_id),
        rows_for(asset_id, dates),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def load_asset_metrics(asset_id):
    path = metrics_path(asset_id)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            rows = json.load(handle)
        if not isinstance(rows, list):
            return {}
        out = {}
        for row in rows:
            day = row.get("date")
            if not day:
                continue
            out[day] = {k: v for k, v in row.items() if k not in ("assetId", "date")}
        return out
    except Exception as exc:
        print(f"Error loading metrics for {asset_id}: {exc}")
        return {}


def incremental_start_date(dates):
    """以既有最新日期的下一天為起點；無可用本機資料時回退完整區間。"""
    valid_days = []
    for day in dates:
        try:
            valid_days.append(datetime.strptime(day, "%Y-%m-%d"))
        except (TypeError, ValueError):
            continue
    if not valid_days:
        return START_STR
    return (max(valid_days) + timedelta(days=1)).strftime("%Y-%m-%d")


def extra_segment_start(local):
    """補 inst/day/margin：從最近有收盤日往回 RECENT_EXTRA_WINDOW。

    不可用 price latest+1：price 已到 calendarLast 時 start 會落在「尚未結算日之後」，
    FinMind 回空 → gap 永不縮 → stall 空轉（約 200 檔卡在只缺 day/inst/margin）。
    """
    closes = []
    for day, vals in local.items():
        if vals.get("close") is None:
            continue
        try:
            closes.append(datetime.strptime(day, "%Y-%m-%d"))
        except (TypeError, ValueError):
            continue
    if not closes:
        return START_STR
    last = max(closes)
    start = last - timedelta(days=RECENT_EXTRA_WINDOW)
    floor = datetime.strptime(START_STR, "%Y-%m-%d")
    if start < floor:
        start = floor
    return start.strftime("%Y-%m-%d")


def last_non_null(rows, key):
    for row in reversed(rows):
        value = row.get(key)
        if value is not None:
            return value
    return None


def merge_prices(asset, rows, into):
    if asset["type"] == "futures":
        by_date = defaultdict(list)
        for item in rows:
            day = item.get("date")
            if not day:
                continue
            if item.get("trading_session") == "after_market":
                continue
            by_date[day].append(item)
        for day, group in by_date.items():
            near = max(group, key=lambda r: float_val(r.get("volume")) or 0)
            into.setdefault(day, {})
            into[day].update({
                "open": float_val(near.get("open")),
                "high": float_val(near.get("max") or near.get("high")),
                "low": float_val(near.get("min") or near.get("low")),
                "close": float_val(near.get("close")),
                "volume": float_val(near.get("volume")),
                "spread": float_val(near.get("spread")),
            })
        return
    for item in rows:
        day = item.get("date")
        if not day:
            continue
        close_val = item.get("close") or item.get("Taiex") or item.get("TPEx")
        into.setdefault(day, {})
        into[day].update({
            "open": float_val(item.get("open")),
            "high": float_val(item.get("max") or item.get("high")),
            "low": float_val(item.get("min") or item.get("low")),
            "close": float_val(close_val),
            "volume": float_val(item.get("Trading_Volume") or item.get("volume")),
            "spread": float_val(item.get("spread")),
        })


def merge_inst(rows, into):
    for item in rows:
        day = item.get("date")
        if not day:
            continue
        into.setdefault(day, {})
        net = (float_val(item.get("buy")) or 0) - (float_val(item.get("sell")) or 0)
        into[day]["instNet"] = (into[day].get("instNet") or 0) + net


def merge_day_trade(rows, into):
    for item in rows:
        day = item.get("date")
        if not day:
            continue
        into.setdefault(day, {})
        into[day]["dayVol"] = float_val(item.get("Volume"))


def merge_margin(rows, into):
    for item in rows:
        day = item.get("date")
        if not day:
            continue
        into.setdefault(day, {})
        today = float_val(item.get("MarginPurchaseTodayBalance")) or 0
        yday = float_val(item.get("MarginPurchaseYesterdayBalance")) or 0
        into[day]["marginDelta"] = today - yday


def normalized_dataset_name(value):
    """正規化 FinMind 的分類名稱，容忍大小寫、底線與空白差異。"""
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def first_float(item, *keys):
    """依候選欄位順序取第一個可轉成數字的值。"""
    for key in keys:
        value = float_val(item.get(key))
        if value is not None:
            return value
    return None


def append_institutional_net(bucket, key, net):
    """累加同日同類法人淨買賣，保留缺值為 null。"""
    previous = bucket.get(key)
    bucket[key] = net if previous is None else previous + net


def build_institutional_rows(rows, limit=250):
    """將全市場法人資料分流為外資、投信與自營商淨買賣。"""
    aliases = {
        "foreigninvestor": "foreignNet",
        "foreigndealerself": "foreignNet",
        "investmenttrust": "trustNet",
        "dealerself": "dealerNet",
        "dealerhedging": "dealerNet",
    }
    by_date = {}
    warned_names = set()
    for item in rows:
        day = item.get("date")
        if not day:
            continue
        raw_name = item.get("name")
        name = normalized_dataset_name(raw_name)
        category = aliases.get(name)
        if category is None:
            warning_key = str(raw_name)
            if warning_key not in warned_names:
                print(f"[!] market_wide 法人資料遇到未知 name：{raw_name!r}")
                warned_names.add(warning_key)
            continue
        values = by_date.setdefault(
            day,
            {"foreignNet": None, "trustNet": None, "dealerNet": None},
        )
        net = (first_float(item, "buy") or 0.0) - (first_float(item, "sell") or 0.0)
        append_institutional_net(values, category, net)

    output = []
    for day in sorted(by_date)[-limit:]:
        values = by_date[day]
        nets = (values["foreignNet"], values["trustNet"], values["dealerNet"])
        output.append({
            "date": day,
            "foreignNet": values["foreignNet"],
            "trustNet": values["trustNet"],
            "dealerNet": values["dealerNet"],
            "totalNet": sum(value for value in nets if value is not None),
        })
    return output


def build_margin_rows(rows, limit=250):
    """將全市場信用交易分流為融資與融券，忽略金額類別資料。"""
    by_date = {}
    warned_names = set()
    for item in rows:
        day = item.get("date")
        if not day:
            continue
        raw_name = item.get("name")
        name = normalized_dataset_name(raw_name)
        if name.endswith("money"):
            continue
        if name == "marginpurchase":
            balance_key = "marginBalance"
            delta_key = "marginDelta"
            today = first_float(
                item, "MarginPurchaseTodayBalance", "TodayBalance", "today_balance"
            )
            yesterday = first_float(
                item, "MarginPurchaseYesterdayBalance", "YesterdayBalance", "yesterday_balance"
            )
        elif name == "shortsale":
            balance_key = "shortBalance"
            delta_key = "shortDelta"
            today = first_float(
                item, "ShortSaleTodayBalance", "TodayBalance", "today_balance"
            )
            yesterday = first_float(
                item, "ShortSaleYesterdayBalance", "YesterdayBalance", "yesterday_balance"
            )
        else:
            warning_key = str(raw_name)
            if warning_key not in warned_names:
                print(f"[!] market_wide 信用交易資料遇到未知 name：{raw_name!r}")
                warned_names.add(warning_key)
            continue
        values = by_date.setdefault(
            day,
            {
                "marginBalance": None,
                "marginDelta": None,
                "shortBalance": None,
                "shortDelta": None,
            },
        )
        values[balance_key] = today
        values[delta_key] = None if today is None or yesterday is None else today - yesterday

    return [
        {"date": day, **by_date[day]}
        for day in sorted(by_date)[-limit:]
    ]


def futures_contract_sort_key(item):
    """依契約月份排序台指期，無契約資訊時才以字串穩定排序。"""
    raw_contract = next(
        (
            item.get(key)
            for key in ("contract_date", "contract", "delivery_month", "expiry_date")
            if item.get(key) not in (None, "")
        ),
        "",
    )
    digits = re.sub(r"\D", "", str(raw_contract))
    return (int(digits) if digits else 99999999, str(raw_contract))


def build_index_rows(rows, is_futures=False, cutoff=None):
    """轉換指數或台指近期貨日 K，輸出固定的 OHLCV schema。"""
    if is_futures:
        grouped = defaultdict(list)
        for item in rows:
            day = item.get("date")
            session = str(item.get("trading_session") or "").lower()
            if day and "after" not in session:
                grouped[day].append(item)
        selected = [
            min(group, key=futures_contract_sort_key)
            for day, group in sorted(grouped.items())
            if group
        ]
    else:
        selected = rows

    output = []
    for item in selected:
        day = item.get("date")
        if not day or (cutoff and day < cutoff):
            continue
        output.append({
            "date": day,
            "open": first_float(item, "open"),
            "high": first_float(item, "max", "high"),
            "low": first_float(item, "min", "low"),
            "close": first_float(item, "close", "Taiex", "TAIEX", "TPEx"),
            "volume": first_float(item, "Trading_Volume", "volume"),
        })
    output.sort(key=lambda item: item["date"])
    return output


def market_wide_index_start(now):
    """取得三年前同日，閏日則退至二月二十八日。"""
    try:
        return now.replace(year=now.year - 3).date().isoformat()
    except ValueError:
        return now.replace(year=now.year - 3, month=2, day=28).date().isoformat()


def build_market_wide():
    """抓取並建立全市場 sidecar；失敗時回報狀態但不阻斷主流程。"""
    try:
        now = taipei_now()
        short_start = (now - timedelta(days=400)).date().isoformat()
        index_start = market_wide_index_start(now)

        institutional = build_institutional_rows(fetch_data(
            "TaiwanStockInstitutionalInvestorsBuySellTotal", None, short_start, END_STR
        ))
        margin = build_margin_rows(fetch_data(
            "TaiwanStockTotalMarginPurchaseShortSale", None, short_start, END_STR
        ))
        indices = {
            "TAIEX": build_index_rows(fetch_data(
                "TaiwanStockPrice", "TAIEX", index_start, END_STR
            ), cutoff=index_start),
            "TPEx": build_index_rows(fetch_data(
                "TaiwanStockPrice", "TPEx", index_start, END_STR
            ), cutoff=index_start),
            "TX": build_index_rows(fetch_data(
                "TaiwanFuturesDaily", "TX", index_start, END_STR
            ), is_futures=True, cutoff=index_start),
        }
        dates = [
            row["date"]
            for rows_for_kind in (institutional, margin, *indices.values())
            for row in rows_for_kind
        ]
        as_of = max(dates) if dates else None
        payload = {
            "generatedAt": now.isoformat(timespec="seconds"),
            "asOf": as_of,
            "institutional": institutional,
            "margin": margin,
            "indices": indices,
        }
        path = os.path.join(DIST_DIR, "market_wide.json")
        atomic_json_dump(path, payload, ensure_ascii=False, separators=(",", ":"))
        print(
            f"Wrote market_wide.json (法人 {len(institutional)}、信用 {len(margin)}、"
            f"指數 {sum(len(rows) for rows in indices.values())} 筆)"
        )
        return {"ok": True, "asOf": as_of}
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        print(f"[!] market_wide.json 建立失敗：{error}")
        return {"ok": False, "error": error}


def schema_ok(asset_id, dates):
    if not dates:
        return False
    days = sorted(dates)
    if days != sorted(set(days)):
        return False
    for day in days:
        if len(day) != 10 or day[4] != "-" or day[7] != "-":
            return False
        if dates[day].get("close") is None:
            continue
    return any(dates[d].get("close") not in (None,) for d in days)


def extra_coverage_ok(asset_id, dates, segment, key, calendar_last):
    if dataset_is_skipped(asset_id, segment):
        return True
    rows = sorted(dates.items())
    recent = [vals for day, vals in rows[-RECENT_EXTRA_WINDOW:] if day <= calendar_last]
    if not recent:
        recent = [vals for _, vals in rows[-RECENT_EXTRA_WINDOW:]]
    return any(vals.get(key) is not None for vals in recent)


def required_segments(asset, market_type):
    """依標的與市場別回傳必須具備的資料區段。"""
    segments = ["price"]
    if asset["type"] not in ("stock", "etf"):
        return segments
    if market_type == "emerging":
        return segments
    if asset["type"] == "etf" and asset["id"].endswith("B"):
        return segments + ["inst"]
    return segments + ["inst", "day", "margin"]


def classify_asset(asset, dates, calendar_days, calendar_last, market_type=None):
    """回傳 (ok, missing_segments, note)。missing 為 price/inst/day/margin/schema。"""
    missing = []
    note = None
    if not schema_ok(asset["id"], dates):
        return False, ["schema", "price"], "invalid_schema"

    closes = [(day, vals) for day, vals in sorted(dates.items()) if vals.get("close") is not None]
    if not closes:
        return False, ["price"], "no_close"

    last_day = closes[-1][0]
    n_close = len(closes)
    cal_n = max(len(calendar_days), 1)

    if last_day >= calendar_last:
        price_ok = True
    elif n_close <= max(20, int(cal_n * DELISTED_MAX_RATIO)) and last_day < calendar_last:
        price_ok = True
        note = "delisted_or_short"
    elif n_close >= int(cal_n * 0.7) and last_day >= calendar_days[max(0, len(calendar_days) - 5)]:
        price_ok = True
        note = "near_calendar"
    else:
        price_ok = False
        missing.append("price")

    required = required_segments(
        asset,
        market_type if market_type is not None else MARKET_TYPES.get(asset["id"], ""),
    )
    if price_ok and note != "delisted_or_short":
        if "inst" in required and not extra_coverage_ok(
            asset["id"], dates, "inst", "instNet", calendar_last
        ):
            missing.append("inst")
        if "day" in required and not extra_coverage_ok(
            asset["id"], dates, "day", "dayVol", calendar_last
        ):
            missing.append("day")
        if "margin" in required and not extra_coverage_ok(
            asset["id"], dates, "margin", "marginDelta", calendar_last
        ):
            missing.append("margin")

    ok = price_ok and not missing
    return ok, missing, note


def is_target_id(sid):
    if not sid:
        return False
    if len(sid) == 4 and sid.isdigit():
        return True
    # ETF：00 開頭、5～6 碼；排除 00xxxA 這類非一般 ETF
    if sid.startswith("00") and 5 <= len(sid) <= 6 and sid[-1] != "A":
        return True
    return False


def get_all_stocks():
    """TaiwanStockInfo 不帶日期；失敗時用快取，不要讓整次抓取直接死掉。"""
    print("Fetching all stock info...")
    try:
        rows = fetch_data("TaiwanStockInfo", None, None, None)
        if rows:
            atomic_json_dump(STOCK_INFO_CACHE, rows, ensure_ascii=False)
            print(f"Cached TaiwanStockInfo ({len(rows)} rows)")
            return rows
    except FetchFailed as exc:
        print(f"[!] TaiwanStockInfo failed: {exc}")
    if os.path.exists(STOCK_INFO_CACHE):
        with open(STOCK_INFO_CACHE, "r", encoding="utf-8") as handle:
            rows = json.load(handle)
        print(f"Using cached TaiwanStockInfo ({len(rows)} rows)")
        return rows
    return []


def process_segments(asset, segments):
    asset_id = asset["id"]
    local = load_asset_metrics(asset_id)
    dataset_price = "TaiwanFuturesDaily" if asset["type"] == "futures" else "TaiwanStockPrice"
    price_start = incremental_start_date(local)

    if "price" in segments or "schema" in segments or not local:
        prices = fetch_data(dataset_price, asset_id, price_start, END_STR)
        record_dataset_result(asset_id, "price", prices)
        merge_prices(asset, prices, local)
        time.sleep(0.05)

    if asset["type"] in ("stock", "etf"):
        # extras 用獨立 lookback，避免 price 已齊時 start 落在結算日之後空打
        extra_start = extra_segment_start(local)
        if "inst" in segments:
            rows = fetch_data(
                "TaiwanStockInstitutionalInvestorsBuySell", asset_id, extra_start, END_STR
            )
            record_dataset_result(asset_id, "inst", rows)
            merge_inst(rows, local)
            time.sleep(0.05)
        if "day" in segments:
            rows = fetch_data("TaiwanStockDayTrading", asset_id, extra_start, END_STR)
            record_dataset_result(asset_id, "day", rows)
            merge_day_trade(rows, local)
            time.sleep(0.05)
        if "margin" in segments:
            rows = fetch_data(
                "TaiwanStockMarginPurchaseShortSale", asset_id, extra_start, END_STR
            )
            record_dataset_result(asset_id, "margin", rows)
            merge_margin(rows, local)
            time.sleep(0.05)

    return asset_id, local


def last_non_null_rows(rows, key):
    return last_non_null(rows, key)


def index_entries_for(assets, asset_ids):
    meta = {a["id"]: a for a in assets}
    entries = []
    for asset_id in sorted(asset_ids):
        dates = load_asset_metrics(asset_id)
        rows = rows_for(asset_id, dates)
        if not rows or rows[-1].get("close") is None:
            continue
        last = rows[-1]
        prev_close = rows[-2].get("close") if len(rows) >= 2 else None
        spread = last.get("spread")
        if spread is None and prev_close is not None:
            spread = last["close"] - prev_close
        info = meta.get(asset_id, {})
        entries.append({
            "id": asset_id,
            "name": info.get("name", asset_id),
            "type": info.get("type", "stock"),
            "latestDate": last["date"],
            "latestClose": last.get("close"),
            "latestSpread": spread or 0.0,
            "latestVolume": last.get("volume"),
            "latestInstNet": last_non_null_rows(rows, "instNet"),
            "latestDayVol": last_non_null_rows(rows, "dayVol"),
            "latestMarginDelta": last_non_null_rows(rows, "marginDelta"),
            "sparkCloses": [r["close"] for r in rows[-SPARK_COUNT:] if r.get("close") is not None],
        })
    return entries


def build_index(assets, complete_ids):
    entries = index_entries_for(assets, complete_ids)
    dest = os.path.join(DIST_DIR, "metrics_index.json")
    atomic_json_dump(dest, entries, ensure_ascii=False, separators=(",", ":"))
    print(f"Wrote dist metrics_index.json ({len(entries)} entries)")
    return entries


def sync_seed_bundle(assets, directory, seed_ids):
    """IPA 只放實際存在的 seed（僅 StockCalendar 專案；公開 repo 略過）。"""
    if not SEED_METRICS_DIR or not APP_SEED_DIR:
        return 0
    present = []
    for asset_id in sorted(BUNDLE_SEED_IDS):
        src = metrics_path(asset_id)
        if asset_id not in seed_ids or not os.path.exists(src) or os.path.getsize(src) <= 2:
            continue
        present.append(asset_id)

    for name in os.listdir(SEED_METRICS_DIR):
        if name.endswith(".json"):
            os.remove(os.path.join(SEED_METRICS_DIR, name))
    for asset_id in present:
        shutil.copyfile(metrics_path(asset_id), os.path.join(SEED_METRICS_DIR, f"{asset_id}.json"))

    present_set = set(present)
    seed_assets = [a for a in assets if a["id"] in present_set]
    seed_dir = []
    seen_dir = set()
    for row in directory:
        sid = row.get("stockId")
        if sid not in present_set or sid in seen_dir:
            continue
        seen_dir.add(sid)
        seed_dir.append(row)
    seed_index = index_entries_for(seed_assets or [{"id": i, "name": i, "type": "stock"} for i in present], present)

    atomic_json_dump(
        os.path.join(RESOURCES_DIR, "assets.json"),
        seed_assets,
        ensure_ascii=False,
        indent=2,
    )
    atomic_json_dump(
        os.path.join(RESOURCES_DIR, "stock_directory.json"),
        seed_dir,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    atomic_json_dump(
        os.path.join(RESOURCES_DIR, "metrics_index.json"),
        seed_index,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    seed_mb = sum(
        os.path.getsize(os.path.join(SEED_METRICS_DIR, name))
        for name in os.listdir(SEED_METRICS_DIR) if name.endswith(".json")
    ) / 1e6
    print(f"Bundle seed: {len(present)} files, {seed_mb:.1f} MB (index {len(seed_index)})")
    return len(present)


def package_outputs(assets, directory, index_entries, complete_ids):
    manifest_entries = []
    for entry in index_entries:
        asset_id = entry["id"]
        src = metrics_path(asset_id)
        if not os.path.exists(src):
            continue
        manifest_entries.append({
            "id": asset_id,
            "latestDate": entry["latestDate"],
            "path": f"metrics/{asset_id}.json",
            "bytes": os.path.getsize(src),
        })

    atomic_json_dump(
        os.path.join(DIST_DIR, "assets.json"),
        assets,
        ensure_ascii=False,
        indent=2,
    )
    atomic_json_dump(
        os.path.join(DIST_DIR, "stock_directory.json"),
        directory,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    manifest = {
        "version": END_STR,
        "generatedAt": taipei_now().isoformat(timespec="seconds"),
        "entries": manifest_entries,
        "sidecars": sidecar_manifest_entries(),
    }
    atomic_json_dump(
        os.path.join(DIST_DIR, "manifest.json"),
        manifest,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    seed_count = sync_seed_bundle(assets, directory, complete_ids & BUNDLE_SEED_IDS)
    total_mb = sum(item["bytes"] for item in manifest_entries) / 1e6
    print(f"Hosting set: {len(manifest_entries)} files, {total_mb:.1f} MB -> {DIST_DIR}")
    return manifest_entries, seed_count


SIDECAR_FILENAMES = (
    "metrics_index.json",
    "assets.json",
    "stock_directory.json",
    "us_index_history.json",
    "us_options_wall.json",
    "us_stock_directory.json",
    "us_metrics_summary.json",
    "market_wide.json",
)


def sidecar_manifest_entries():
    """回傳現有 sidecar 的檔名、位元組與 SHA-256 校驗值。"""
    entries = []
    for name in SIDECAR_FILENAMES:
        path = os.path.join(DIST_DIR, name)
        if not os.path.isfile(path):
            print(f"[!] manifest sidecar 尚未產生：{name}")
            continue
        digest = sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        entries.append({
            "name": name,
            "bytes": os.path.getsize(path),
            "sha256": digest.hexdigest(),
        })
    return entries


def write_status(payload):
    path = os.path.join(DIST_DIR, "status.json")
    atomic_json_dump(path, payload, ensure_ascii=False, indent=2)


def write_market_wide_status(result):
    """在全市場資料完成或失敗後立即更新 status，避免後續流程中斷而漏記。"""
    path = os.path.join(DIST_DIR, "status.json")
    payload = {}
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                existing = json.load(handle)
            if isinstance(existing, dict):
                payload = existing
        except Exception as exc:
            print(f"[!] 無法讀取既有 status.json，將重建：{exc}")
    payload["marketWide"] = result
    payload["updatedAt"] = taipei_now().isoformat(timespec="seconds")
    write_status(payload)


def write_incomplete_report(gaps):
    path = os.path.join(DIST_DIR, "incomplete_report.json")
    atomic_json_dump(path, gaps, ensure_ascii=False, indent=2)
    print(f"Wrote incomplete_report.json ({len(gaps)} remaining)")


def verify_universe(assets, directory):
    expected_ids = {a["id"] for a in assets}
    dir_ids = {row["stockId"] for row in directory}
    stock_ids = {a["id"] for a in assets if a["type"] in ("stock", "etf")}
    missing_dir = sorted(stock_ids - dir_ids)
    extra = []
    if missing_dir:
        extra.append(f"directory missing {len(missing_dir)}")
    return not extra, extra, expected_ids


def ensure_calendar(assets):
    taiex = load_asset_metrics("TAIEX")
    closes = sorted(day for day, vals in taiex.items() if vals.get("close") is not None)
    if len(closes) >= 200:
        return closes, closes[-1]
    print("Fetching TAIEX calendar first...")
    _, local = process_segments({"id": "TAIEX", "name": "加權指數", "type": "index"}, ["price"])
    with metrics_lock:
        save_asset_metrics("TAIEX", local)
    closes = sorted(day for day, vals in local.items() if vals.get("close") is not None)
    if not closes:
        raise FetchFailed("cannot build TAIEX calendar")
    return closes, closes[-1]


def assets_and_directory_from_rows(raw_stocks):
    filtered = [row for row in raw_stocks if is_target_id(row.get("stock_id", ""))]
    assets = []
    seen = set()
    for row in filtered:
        sid = row.get("stock_id", "")
        if not sid or sid in seen:
            continue
        seen.add(sid)
        assets.append({
            "id": sid,
            "name": row.get("stock_name", ""),
            "type": "etf" if sid.startswith("00") else "stock",
        })
    for item in (
        {"id": "TAIEX", "name": "加權指數", "type": "index"},
        {"id": "TPEx", "name": "櫃買指數", "type": "index"},
        {"id": "TX", "name": "台指期", "type": "futures"},
    ):
        if item["id"] not in seen:
            assets.append(item)
            seen.add(item["id"])

    directory = []
    seen_dir = set()
    for row in filtered:
        sid = row.get("stock_id", "")
        if not sid or sid in seen_dir:
            continue
        seen_dir.add(sid)
        directory.append({
            "stockId": sid,
            "stockName": row.get("stock_name", ""),
            "type": row.get("type", ""),
            "industry": row.get("industry_category", ""),
        })
    return assets, directory


def load_cached_universe():
    assets_path = os.path.join(DIST_DIR, "assets.json")
    dir_path = os.path.join(DIST_DIR, "stock_directory.json")
    if not os.path.exists(assets_path):
        return None, None
    with open(assets_path, "r", encoding="utf-8") as handle:
        assets = json.load(handle)
    if not isinstance(assets, list) or len(assets) < 100:
        return None, None
    directory = []
    if os.path.exists(dir_path):
        with open(dir_path, "r", encoding="utf-8") as handle:
            directory = json.load(handle)
    asset_ids = {a["id"] for a in assets}
    deduped = []
    seen_dir = set()
    for row in directory:
        sid = row.get("stockId")
        if not sid or sid not in asset_ids or sid in seen_dir:
            continue
        seen_dir.add(sid)
        deduped.append(row)
    directory = deduped
    have_dir = {row["stockId"] for row in directory}
    for asset in assets:
        if asset["type"] in ("stock", "etf") and asset["id"] not in have_dir:
            directory.append({
                "stockId": asset["id"],
                "stockName": asset.get("name", ""),
                "type": asset.get("type", ""),
                "industry": "",
            })
    return assets, directory


def existing_seed_ids():
    return {
        asset_id
        for asset_id in BUNDLE_SEED_IDS
        if os.path.exists(metrics_path(asset_id)) and os.path.getsize(metrics_path(asset_id)) > 2
    }


def run_wave(jobs):
    """jobs: list of (asset, segments). 回傳成功寫入數。"""
    if not jobs:
        return 0
    written = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_segments, asset, segments): (asset, segments)
            for asset, segments in jobs
        }
        done = 0
        total = len(futures)
        for future in as_completed(futures):
            asset, segments = futures[future]
            done += 1
            try:
                asset_id, local = future.result()
                with metrics_lock:
                    save_asset_metrics(asset_id, local)
                written += 1
                print(f"[{done}/{total}] saved {asset_id} segs={','.join(segments)} days={len(local)}")
            except Exception as exc:
                print(f"[{done}/{total}] FAIL {asset['id']}: {exc}")
    return written


def main():
    global MARKET_TYPES
    print(f"Fetching data from {START_STR} to {END_STR}")
    assets, directory = load_cached_universe()
    if assets:
        print(f"Using cached universe ({len(assets)} assets); skip TaiwanStockInfo")
    else:
        raw_stocks = get_all_stocks()
        if raw_stocks:
            assets, directory = assets_and_directory_from_rows(raw_stocks)
        if not assets:
            raise FetchFailed("no stock universe (TaiwanStockInfo failed and no cache)")

    print(f"Total target universe: {len(assets)} assets")
    MARKET_TYPES = load_market_types(directory)
    atomic_json_dump(
        os.path.join(DIST_DIR, "assets.json"),
        assets,
        ensure_ascii=False,
        indent=2,
    )
    atomic_json_dump(
        os.path.join(DIST_DIR, "stock_directory.json"),
        directory,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    print(f"Wrote dist stock_directory.json ({len(directory)} entries)")

    sync_seed_bundle(assets, directory, existing_seed_ids())
    market_wide_status = build_market_wide()
    write_market_wide_status(market_wide_status)

    calendar_days, calendar_last = ensure_calendar(assets)
    print(f"Calendar {calendar_days[0]} -> {calendar_last} ({len(calendar_days)} sessions)")

    universe_ok, universe_issues, expected_ids = verify_universe(assets, directory)
    if not universe_ok:
        print("Universe issues:", universe_issues)

    stall_rounds = 0
    last_missing = None
    round_idx = 0

    while True:
        round_idx += 1
        complete_ids = set()
        delisted_ids = set()
        gaps = []
        jobs_map = {}

        for asset in assets:
            dates = load_asset_metrics(asset["id"])
            ok, missing, note = classify_asset(asset, dates, calendar_days, calendar_last)
            if ok:
                complete_ids.add(asset["id"])
                if note == "delisted_or_short":
                    delisted_ids.add(asset["id"])
                continue
            gaps.append({"id": asset["id"], "missing": missing, "note": note})
            jobs_map[asset["id"]] = (asset, missing or ["price"])

        coverage = 100.0 * len(complete_ids) / max(len(assets), 1)
        write_status({
            "round": round_idx,
            "universe": len(assets),
            "complete": len(complete_ids),
            "delistedOrShort": len(delisted_ids),
            "missing": len(gaps),
            "coveragePercent": round(coverage, 2),
            "calendarLast": calendar_last,
            "updatedAt": taipei_now().isoformat(timespec="seconds"),
            "marketWide": market_wide_status,
        })
        print(
            f"== round {round_idx} complete {len(complete_ids)}/{len(assets)} "
            f"({coverage:.1f}%) delisted/short {len(delisted_ids)} missing {len(gaps)}"
        )

        schema_bad = [g for g in gaps if "schema" in g["missing"]]
        if schema_bad:
            print(f"  schema failures: {len(schema_bad)}")

        price_ok_seed = existing_seed_ids() | (complete_ids & BUNDLE_SEED_IDS)
        sync_seed_bundle(assets, directory, price_ok_seed)

        if not gaps and len(complete_ids) == len(assets):
            index_entries = build_index(assets, complete_ids)
            manifest_entries, _ = package_outputs(assets, directory, index_entries, complete_ids)
            if len(manifest_entries) != len(assets):
                print(f"Manifest mismatch {len(manifest_entries)} vs universe {len(assets)}")
                write_incomplete_report(gaps)
                stall_rounds += 1
            else:
                report_path = os.path.join(DIST_DIR, "incomplete_report.json")
                if os.path.exists(report_path):
                    os.remove(report_path)
                print(f"COMPLETE {len(assets)} assets verified {END_STR}")
                return

        missing_key = tuple(sorted((g["id"], tuple(g["missing"])) for g in gaps))
        if missing_key == last_missing:
            stall_rounds += 1
        else:
            stall_rounds = 0
            last_missing = missing_key

        if stall_rounds >= STALL_ROUNDS_LIMIT:
            write_incomplete_report(gaps)
            print(
                f"No progress after {round_idx} rounds ({len(gaps)} incomplete). "
                "Waiting 30m for FinMind quota reset. Not COMPLETE."
            )
            time.sleep(30 * 60)
            stall_rounds = 0
            continue

        # seed 標的優先，讓 IPA 子集盡早齊
        jobs = list(jobs_map.values())
        jobs.sort(key=lambda item: (item[0]["id"] not in BUNDLE_SEED_IDS, item[0]["id"]))
        run_wave(jobs)
        calendar_days, calendar_last = ensure_calendar(assets)


if __name__ == "__main__":
    main()
