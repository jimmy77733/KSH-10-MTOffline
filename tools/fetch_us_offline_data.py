#!/usr/bin/env python3
"""
fetch_us_offline_data.py
美股前 200 大標的歷史資料高效抓取器（支援多組 Finnhub Token 自動輪替、多執行緒並行與限流管理）
- 抓取美股前 200 大權值股、大盤指數 (SPY, QQQ, DIA, ^VIX) 及板塊 ETF
- 支援 Finnhub 多 Token 自動輪替 (Round-Robin & Rate Limit 429 故障轉移)
- 抓取歷史日 K、基本面指標 (PE, PB, 52W, 市值)
- 輸出至 dist/offline 目錄
- 注意: 不再使用 Yahoo Finance API (違反其 ToS)
"""

import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DIST_DIR = os.environ.get("MT_OFFLINE_ROOT", os.path.join(REPO_ROOT, "dist", "offline"))
METRICS_DIR = os.path.join(DIST_DIR, "metrics")

os.makedirs(DIST_DIR, exist_ok=True)
os.makedirs(METRICS_DIR, exist_ok=True)

USER_AGENT = "MT-Offline-Fetcher/1.0"

# 美股前 200 大權值與代表性標的
US_TOP_UNIVERSE = [
    # Top Tech & Giants (M7 + Leaders)
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO", "ORCL", "CRM",
    "AMD", "ADBE", "CSCO", "IBM", "QCOM", "TXN", "NOW", "AMAT", "INTU", "LRCX",
    "PANW", "MU", "KLAC", "SNPS", "CDNS", "ADI", "ANET", "NXPI", "MRVL", "FTNT",
    "PLTR", "ARM", "SMCI", "COIN", "TSM", "BABA", "ASML", "SAP", "PDD", "BIDU",
    
    # Financials
    "BRK.B", "JPM", "V", "MA", "BAC", "WFC", "GS", "MS", "SPGI", "BLK",
    "PGR", "CB", "MMC", "C", "AXP", "MCO", "ICE", "USB", "PNC", "TRV",
    "AON", "CME", "SCHW", "AFL", "MET", "ALL", "BK", "COF", "PRU", "AIG",
    
    # Healthcare & Biotech
    "LLY", "UNH", "JNJ", "ABBV", "MRK", "TMO", "ABT", "ISRG", "PFE", "AMGN",
    "SYK", "VRTX", "ELV", "BMY", "MDT", "BSX", "CI", "GILD", "REGN", "ZTS",
    "BDX", "CVS", "HCA", "MCK", "COR", "EW", "HUM", "DXCM", "IDXX", "IQV",
    
    # Consumer & Retail
    "COST", "HD", "PG", "WMT", "NFLX", "KO", "PEP", "MCD", "DIS", "PM",
    "BKNG", "LOW", "UBER", "TJX", "MDLZ", "MO", "CL", "NKE", "TGT", "SBUX",
    "ABNB", "MAR", "LULU", "HLT", "ORLY", "AZO", "CMG", "DG", "DLTR", "ROST",
    
    # Industrials & Defense
    "GE", "CAT", "HON", "RTX", "UNP", "DE", "ETN", "LMT", "BA", "ITW",
    "TDG", "WM", "SHW", "EMR", "PH", "GD", "NOC", "CSX", "NSC", "PCAR",
    "TT", "URI", "FDX", "UPS", "CP", "CNI", "CARR", "OTIS", "JCI", "CMI",
    
    # Energy
    "XOM", "CVX", "COP", "EOG", "SLB", "MPC", "PSX", "VLO", "OXY", "HES",
    "WMB", "KMI", "OKE", "HAL", "BKR", "FANG", "TRGP", "DVN", "EQT", "MRO",
    
    # Communication & Telecom
    "VZ", "CMCSA", "T", "TMUS", "CHTR", "EA", "TTWO", "WBD", "OMC", "IPG",
    
    # Utilities & Real Estate
    "NEE", "SO", "DUK", "CEG", "SRE", "AEP", "VST", "D", "EXC", "XEL",
    "PLD", "AMT", "EQIX", "CCI", "PSA", "O", "WELL", "DLR", "SPG", "VICI",
    
    # Materials
    "LIN", "APD", "ECL", "FCX", "NEM", "NUE", "CTVA", "DOW", "DD", "ALB",
    
    # Major Market & Sector ETFs
    "SPY", "QQQ", "DIA", "IWM", "VIX", "VOO", "VTI", "XLK", "XLF", "XLV",
    "XLY", "XLC", "XLI", "XLP", "XLE", "XLRE", "XLU", "XLB", "SMH", "SOXX",
    "ARKK", "TLT", "GLD", "SLV", "USO", "HYG", "LQD", "EEM", "EFA", "VNQ"
]


class FinnhubTokenManager:
    """管理多組 Finnhub Token 輪替與配額防護 (Thread-safe)"""
    def __init__(self):
        self.tokens = []
        self.current_idx = 0
        self.cooldowns = {}
        self.lock = threading.Lock()
        self.load_tokens()
        
    def load_tokens(self):
        token_file = os.path.join(SCRIPT_DIR, "finnhub_tokens.local.json")
        if not os.path.exists(token_file):
            token_file = os.path.join(REPO_ROOT, "tools", "finnhub_tokens.local.json")
            
        if os.path.exists(token_file):
            try:
                with open(token_file, "r", encoding="utf-8") as f:
                    entries = json.load(f)
                    for item in entries:
                        if isinstance(item, dict) and item.get("token"):
                            self.tokens.append(item["token"].strip())
                        elif isinstance(item, str) and item.strip():
                            self.tokens.append(item.strip())
            except Exception as e:
                print(f"[Warning] Failed to load {token_file}: {e}")
                
        env_tokens = os.environ.get("FINNHUB_TOKENS_JSON", "").strip()
        if env_tokens:
            try:
                entries = json.loads(env_tokens)
                for item in entries:
                    if isinstance(item, dict) and item.get("token"):
                        self.tokens.append(item["token"].strip())
                    elif isinstance(item, str) and item.strip():
                        self.tokens.append(item.strip())
            except Exception:
                pass
                
        env_csv = os.environ.get("FINNHUB_TOKENS", "").strip()
        if env_csv:
            for t in env_csv.split(","):
                if t.strip():
                    self.tokens.append(t.strip())
                    
        self.tokens = list(dict.fromkeys(self.tokens))
        print(f"Loaded {len(self.tokens)} Finnhub tokens for rotation.")

    def get_active_token(self) -> str:
        with self.lock:
            if not self.tokens:
                return ""
            now = time.time()
            for _ in range(len(self.tokens)):
                t = self.tokens[self.current_idx]
                if self.cooldowns.get(t, 0) <= now:
                    self.current_idx = (self.current_idx + 1) % len(self.tokens)
                    return t
                self.current_idx = (self.current_idx + 1) % len(self.tokens)
                
            earliest_token = min(self.cooldowns, key=self.cooldowns.get)
            return earliest_token

    def mark_rate_limited(self, token: str):
        with self.lock:
            print(f"  [RateLimit] Token ...{token[-6:]} hit limit (429), cooling down for 30s.")
            self.cooldowns[token] = time.time() + 30

    def query(self, endpoint: str, params: dict = None) -> dict:
        params = params or {}
        token = self.get_active_token()
        if not token:
            return {}
            
        params["token"] = token
        url = f"https://finnhub.io/api/v1/{endpoint}?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        
        for attempt in range(max(len(self.tokens), 2)):
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    if resp.status == 200:
                        return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    self.mark_rate_limited(token)
                    token = self.get_active_token()
                    params["token"] = token
                    url = f"https://finnhub.io/api/v1/{endpoint}?" + urllib.parse.urlencode(params)
                    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
                    continue
                break
            except Exception:
                break
        return {}


def fetch_finnhub_candles(symbol: str, finnhub: 'FinnhubTokenManager', years: int = 3) -> list:
    """使用 Finnhub API 抓取歷史日 K 線（約 3 年）"""
    clean_sym = symbol.replace(".B", "").replace(".A", "")
    
    # Finnhub 需要 unix timestamp (秒)
    end = int(time.time())
    start = end - (years * 365 * 24 * 3600)
    
    data = finnhub.query("stock/candle", {
        "symbol": clean_sym,
        "resolution": "D",
        "from": start,
        "to": end
    })
    
    if not data or data.get("s") != "ok":
        return []
    
    timestamps = data.get("t", [])
    opens = data.get("o", [])
    highs = data.get("h", [])
    lows = data.get("l", [])
    closes = data.get("c", [])
    volumes = data.get("v", [])
    
    bars = []
    for i in range(len(timestamps)):
        if i >= len(closes) or closes[i] is None or closes[i] <= 0:
            continue
        date_str = datetime.fromtimestamp(timestamps[i]).strftime("%Y-%m-%d")
        c = closes[i]
        o = opens[i] if i < len(opens) and opens[i] is not None else c
        h = highs[i] if i < len(highs) and highs[i] is not None else c
        l = lows[i] if i < len(lows) and lows[i] is not None else c
        v = volumes[i] if i < len(volumes) and volumes[i] is not None else 0
        
        bars.append({
            "date": date_str,
            "open": round(float(o), 2),
            "high": round(float(h), 2),
            "low": round(float(l), 2),
            "close": round(float(c), 2),
            "volume": int(v) if v else 0
        })
    
    return bars


def process_single_stock(symbol: str, finnhub: FinnhubTokenManager):
    clean_id = symbol.replace("^", "").replace("-", ".")
    
    # 1. 使用 Finnhub API 抓取日 K 線 (3 年)
    bars = fetch_finnhub_candles(symbol, finnhub, years=3)
    if bars:
        metric_file = os.path.join(METRICS_DIR, f"{clean_id}.json")
        with open(metric_file, "w", encoding="utf-8") as f:
            json.dump(bars, f, ensure_ascii=False, separators=(',', ':'))
            
    # 2. 透過 Finnhub 輪替 Token 抓取公司基本面
    profile = {}
    metric = {}
    if finnhub.tokens and not symbol.startswith(("XL", "SMH", "SOXX", "IWM", "VOO", "VTI", "GLD", "SLV", "USO", "TLT")):
        profile = finnhub.query("stock/profile2", {"symbol": clean_id.replace(".B", "")}) or {}
        metric = finnhub.query("stock/metric", {"symbol": clean_id.replace(".B", ""), "metric": "all"}) or {}
        
    comp_name = profile.get("name") or symbol
    industry = profile.get("finnhubIndustry") or ("ETF" if symbol.startswith("X") or symbol in ["SPY", "QQQ", "DIA", "IWM"] else "US Equity")
    
    dir_entry = {
        "stockId": clean_id,
        "stockName": f"{clean_id} ({comp_name})" if comp_name != clean_id else clean_id,
        "type": "us",
        "industry": industry
    }
    
    metric_entry = None
    if metric.get("metric"):
        m = metric["metric"]
        metric_entry = {
            "pe": m.get("peNormalizedAnnual") or m.get("peTTM"),
            "pb": m.get("pbAnnual") or m.get("pbTTM"),
            "beta": m.get("beta"),
            "52High": m.get("52WeekHigh"),
            "52Low": m.get("52WeekLow"),
            "marketCap": profile.get("marketCapitalization")
        }
        
    return clean_id, symbol, bars, dir_entry, metric_entry


def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting Concurrent US Top 200 Offline Data Fetch...")
    finnhub = FinnhubTokenManager()
    
    index_history = {}
    us_directory = []
    us_metrics_summary = {}
    
    total = len(US_TOP_UNIVERSE)
    print(f"Target Universe: {total} US stocks & ETFs. Concurrency: 8 workers.")
    
    start_time = time.time()
    completed = 0
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_single_stock, sym, finnhub): sym for sym in US_TOP_UNIVERSE}
        for future in as_completed(futures):
            completed += 1
            sym = futures[future]
            try:
                clean_id, symbol, bars, dir_entry, metric_entry = future.result()
                us_directory.append(dir_entry)
                if metric_entry:
                    us_metrics_summary[clean_id] = metric_entry
                    
                if symbol in ["SPY", "QQQ", "DIA", "VIX", "^VIX"] and bars:
                    idx_key = "SPY" if symbol == "SPY" else ("QQQ" if symbol == "QQQ" else ("DIA" if symbol == "DIA" else "VIX"))
                    index_history[idx_key] = [
                        {
                            "date": b["date"],
                            "open": b["open"],
                            "high": b["high"],
                            "low": b["low"],
                            "close": b["close"],
                            "volume": b["volume"]
                        }
                        for b in bars
                    ]
                print(f"[{completed}/{total}] ✓ {sym} ({len(bars)} bars)")
            except Exception as e:
                print(f"[{completed}/{total}] ✗ Error processing {sym}: {e}")
                
    elapsed = time.time() - start_time
    print(f"All {total} US stocks processed in {elapsed:.1f}s.")
    
    # 按照字典順序排序
    us_directory.sort(key=lambda x: x["stockId"])
    
    # 儲存美股四大指數總檔
    index_file = os.path.join(DIST_DIR, "us_index_history.json")
    with open(index_file, "w", encoding="utf-8") as f:
        json.dump(index_history, f, ensure_ascii=False, separators=(',', ':'))
    print(f"✓ Wrote {index_file} ({len(index_history)} indices)")
    
    # 儲存美股字典檔
    us_dir_file = os.path.join(DIST_DIR, "us_stock_directory.json")
    with open(us_dir_file, "w", encoding="utf-8") as f:
        json.dump(us_directory, f, ensure_ascii=False, indent=2)
    print(f"✓ Wrote {us_dir_file} ({len(us_directory)} symbols)")
    
    # 儲存基本面指標摘要
    if us_metrics_summary:
        metric_sum_file = os.path.join(DIST_DIR, "us_metrics_summary.json")
        with open(metric_sum_file, "w", encoding="utf-8") as f:
            json.dump(us_metrics_summary, f, ensure_ascii=False, indent=2)
        print(f"✓ Wrote {metric_sum_file} ({len(us_metrics_summary)} companies)")
        
    # Options wall: Finnhub free tier 不提供 options 資料，保留既有 us_options_wall.json 不更新
    print("  [Info] Options wall not refreshed (no official free Finnhub endpoint; existing us_options_wall.json untouched)")
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] US Top 200 Offline Data Fetch Complete!")


if __name__ == "__main__":
    main()
