import json
import os
import shutil
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

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

end_date = datetime.now()
start_date = end_date - timedelta(days=1825)
START_STR = start_date.strftime("%Y-%m-%d")
END_STR = end_date.strftime("%Y-%m-%d")

metrics_lock = threading.Lock()


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
    with open(metrics_path(asset_id), "w", encoding="utf-8") as handle:
        json.dump(rows_for(asset_id, dates), handle, ensure_ascii=False, separators=(",", ":"))


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


def extra_coverage_ok(dates, key, calendar_last):
    rows = sorted(dates.items())
    recent = [vals for day, vals in rows[-RECENT_EXTRA_WINDOW:] if day <= calendar_last]
    if not recent:
        recent = [vals for _, vals in rows[-RECENT_EXTRA_WINDOW:]]
    return any(vals.get(key) is not None for vals in recent)


def classify_asset(asset, dates, calendar_days, calendar_last):
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

    if asset["type"] in ("stock", "etf") and price_ok and note != "delisted_or_short":
        if not extra_coverage_ok(dates, "instNet", calendar_last):
            missing.append("inst")
        if not extra_coverage_ok(dates, "dayVol", calendar_last):
            missing.append("day")
        if not extra_coverage_ok(dates, "marginDelta", calendar_last):
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
            with open(STOCK_INFO_CACHE, "w", encoding="utf-8") as handle:
                json.dump(rows, handle, ensure_ascii=False)
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

    if "price" in segments or "schema" in segments or not local:
        prices = fetch_data(dataset_price, asset_id, START_STR, END_STR)
        merge_prices(asset, prices, local)
        time.sleep(0.05)

    if asset["type"] in ("stock", "etf"):
        if "inst" in segments:
            merge_inst(fetch_data("TaiwanStockInstitutionalInvestorsBuySell", asset_id, START_STR, END_STR), local)
            time.sleep(0.05)
        if "day" in segments:
            merge_day_trade(fetch_data("TaiwanStockDayTrading", asset_id, START_STR, END_STR), local)
            time.sleep(0.05)
        if "margin" in segments:
            merge_margin(fetch_data("TaiwanStockMarginPurchaseShortSale", asset_id, START_STR, END_STR), local)
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
    with open(dest, "w", encoding="utf-8") as handle:
        json.dump(entries, handle, ensure_ascii=False, separators=(",", ":"))
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

    with open(os.path.join(RESOURCES_DIR, "assets.json"), "w", encoding="utf-8") as handle:
        json.dump(seed_assets, handle, ensure_ascii=False, indent=2)
    with open(os.path.join(RESOURCES_DIR, "stock_directory.json"), "w", encoding="utf-8") as handle:
        json.dump(seed_dir, handle, ensure_ascii=False, separators=(",", ":"))
    with open(os.path.join(RESOURCES_DIR, "metrics_index.json"), "w", encoding="utf-8") as handle:
        json.dump(seed_index, handle, ensure_ascii=False, separators=(",", ":"))

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

    manifest = {
        "version": END_STR,
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
        "entries": manifest_entries,
    }
    with open(os.path.join(DIST_DIR, "manifest.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, separators=(",", ":"))

    with open(os.path.join(DIST_DIR, "assets.json"), "w", encoding="utf-8") as handle:
        json.dump(assets, handle, ensure_ascii=False, indent=2)
    with open(os.path.join(DIST_DIR, "stock_directory.json"), "w", encoding="utf-8") as handle:
        json.dump(directory, handle, ensure_ascii=False, separators=(",", ":"))

    seed_count = sync_seed_bundle(assets, directory, complete_ids & BUNDLE_SEED_IDS)
    total_mb = sum(item["bytes"] for item in manifest_entries) / 1e6
    print(f"Hosting set: {len(manifest_entries)} files, {total_mb:.1f} MB -> {DIST_DIR}")
    return manifest_entries, seed_count


def write_status(payload):
    path = os.path.join(DIST_DIR, "status.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def write_incomplete_report(gaps):
    path = os.path.join(DIST_DIR, "incomplete_report.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(gaps, handle, ensure_ascii=False, indent=2)
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
    with open(os.path.join(DIST_DIR, "assets.json"), "w", encoding="utf-8") as handle:
        json.dump(assets, handle, ensure_ascii=False, indent=2)
    with open(os.path.join(DIST_DIR, "stock_directory.json"), "w", encoding="utf-8") as handle:
        json.dump(directory, handle, ensure_ascii=False, separators=(",", ":"))
    print(f"Wrote dist stock_directory.json ({len(directory)} entries)")

    sync_seed_bundle(assets, directory, existing_seed_ids())

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
            "updatedAt": datetime.now().isoformat(timespec="seconds"),
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
