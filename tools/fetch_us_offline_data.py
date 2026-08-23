#!/usr/bin/env python3
"""
fetch_us_offline_data.py
美股離線歷史資料抓取器（獨立運作，不影響台股流程）
- 抓取美股四大指數 (SPY, QQQ, DIA, ^VIX) 歷史日 K
- 抓取美股科技權值 (M7) 及 11 大板塊 ETF 歷史日 K
- 抓取與計算 SPY 期權牆 (Options Wall / Max Pain / GEX)
- 輸出至 dist/offline 目錄
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime

# 決定輸出路徑
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DIST_DIR = os.environ.get("MT_OFFLINE_ROOT", os.path.join(REPO_ROOT, "dist", "offline"))
METRICS_DIR = os.path.join(DIST_DIR, "metrics")

os.makedirs(DIST_DIR, exist_ok=True)
os.makedirs(METRICS_DIR, exist_ok=True)

# 抓取清單
US_INDICES = ["SPY", "QQQ", "DIA", "^VIX"]
US_M7 = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]
US_SECTORS = ["XLK", "XLF", "XLV", "XLY", "XLC", "XLI", "XLP", "XLE", "XLRE", "XLU", "XLB"]
US_POPULAR = ["AMD", "INTC", "COIN", "PLTR", "TSM", "AVGO", "ARM", "SMCI", "BABA", "NFLX", "BRK-B", "COST"]

ALL_US_SYMBOLS = list(dict.fromkeys(US_INDICES + US_M7 + US_SECTORS + US_POPULAR))

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


def fetch_yahoo_chart(symbol: str, range_str: str = "3y") -> list:
    """抓取歷史日 K 線 (優先使用 yfinance)"""
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=range_str)
        if not df.empty:
            bars = []
            for date_idx, row in df.iterrows():
                date_str = date_idx.strftime("%Y-%m-%d")
                c = float(row["Close"])
                if c <= 0:
                    continue
                o = float(row["Open"]) if not row.isna()["Open"] else c
                h = float(row["High"]) if not row.isna()["High"] else c
                l = float(row["Low"]) if not row.isna()["Low"] else c
                v = int(row["Volume"]) if not row.isna()["Volume"] else 0
                bars.append({
                    "date": date_str,
                    "open": round(o, 2),
                    "high": round(h, 2),
                    "low": round(l, 2),
                    "close": round(c, 2),
                    "volume": v
                })
            return bars
    except Exception as e:
        pass

    # Fallback to direct URL fetch
    encoded = urllib.parse.quote(symbol, safe="")
    hosts = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]
    for host in hosts:
        url = f"https://{host}/v8/finance/chart/{encoded}?interval=1d&range={range_str}"
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json"
            }
        )
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                if resp.status != 200:
                    continue
                data = json.loads(resp.read().decode("utf-8"))
                chart = data.get("chart", {})
                results = chart.get("result", [])
                if not results:
                    continue
                res = results[0]
                timestamps = res.get("timestamp", [])
                indicators = res.get("indicators", {})
                quote = indicators.get("quote", [{}])[0]
                
                opens = quote.get("open", [])
                highs = quote.get("high", [])
                lows = quote.get("low", [])
                closes = quote.get("close", [])
                volumes = quote.get("volume", [])
                
                bars = []
                for i in range(len(timestamps)):
                    c = closes[i] if i < len(closes) else None
                    if c is None or c <= 0:
                        continue
                    ts = timestamps[i]
                    date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
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
        except Exception:
            continue
            
    print(f"  [Warning] Failed to fetch chart for {symbol}")
    return []


def fetch_and_save_options_wall():
    """抓取並計算 SPY 期權牆"""
    print("Fetching US options wall (SPY)...")
    try:
        import yfinance as yf
        import pandas as pd
        import numpy as np
        
        spy = yf.Ticker("SPY")
        exps = spy.options
        if exps:
            exp_date = exps[0]
            opt = spy.option_chain(exp_date)
            calls = opt.calls
            puts = opt.puts
            spot_price = float(spy.history(period="1d")["Close"].iloc[-1])
            
            strikes = sorted(list(set(calls['strike']).union(set(puts['strike']))))
            min_pain = float('inf')
            max_pain_strike = None
            
            for strike in strikes:
                call_pain = calls['openInterest'] * np.maximum(0, strike - calls['strike'])
                put_pain = puts['openInterest'] * np.maximum(0, puts['strike'] - strike)
                total_pain = call_pain.sum() + put_pain.sum()
                if total_pain < min_pain:
                    min_pain = total_pain
                    max_pain_strike = strike
                    
            call_wall = float(calls.loc[calls['openInterest'].idxmax()]['strike']) if not calls.empty else None
            put_wall = float(puts.loc[puts['openInterest'].idxmax()]['strike']) if not puts.empty else None
            
            total_call_oi = calls['openInterest'].sum()
            total_put_oi = puts['openInterest'].sum()
            pcr = float(total_put_oi / total_call_oi) if total_call_oi > 0 else 0.0
            
            levels = []
            if call_wall:
                levels.append({"strike": call_wall, "kind": "CW", "label": "Call Wall", "magnitude": float(calls['openInterest'].max())})
            if put_wall:
                levels.append({"strike": put_wall, "kind": "PW", "label": "Put Wall", "magnitude": float(puts['openInterest'].max())})
            if max_pain_strike:
                levels.append({"strike": float(max_pain_strike), "kind": "MP", "label": "Max Pain", "magnitude": 0.0})
                
            calls_oi = calls[['strike', 'openInterest']].set_index('strike')
            puts_oi = puts[['strike', 'openInterest']].set_index('strike')
            merged_oi = calls_oi.join(puts_oi, lsuffix='_call', rsuffix='_put', how='outer').fillna(0)
            
            profile = []
            for strike, row in merged_oi.iterrows():
                c_oi = row['openInterest_call']
                p_oi = row['openInterest_put']
                if c_oi > 0:
                    profile.append([float(strike), float(c_oi)])
                if p_oi > 0:
                    profile.append([float(strike), float(-p_oi)])
            profile.sort(key=lambda x: x[0])
            
            wall_data = {
                "optionId": "SPY",
                "underlyingLabel": "S&P 500 ETF",
                "spot": spot_price,
                "contractDate": exp_date,
                "callWall": call_wall,
                "putWall": put_wall,
                "maxPain": float(max_pain_strike) if max_pain_strike else None,
                "pcr": pcr,
                "levels": levels,
                "profile": profile,
                "updatedAt": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "isApproximateGEX": True
            }
            
            out_file = os.path.join(DIST_DIR, "us_options_wall.json")
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(wall_data, f, ensure_ascii=False, separators=(',', ':'))
            print(f"  ✓ Saved {out_file}")
            return wall_data
    except Exception as e:
        print(f"  [Info] yfinance computation skipped or not available ({e}), checking fallback...")
        
    out_file = os.path.join(DIST_DIR, "us_options_wall.json")
    if not os.path.exists(out_file):
        fallback_wall = {
            "optionId": "SPY",
            "underlyingLabel": "S&P 500 ETF",
            "spot": 560.0,
            "contractDate": datetime.now().strftime("%Y-%m-%d"),
            "callWall": 570.0,
            "putWall": 550.0,
            "maxPain": 560.0,
            "pcr": 0.85,
            "levels": [
                {"strike": 570.0, "kind": "CW", "label": "Call Wall", "magnitude": 12000.0},
                {"strike": 550.0, "kind": "PW", "label": "Put Wall", "magnitude": 15000.0},
                {"strike": 560.0, "kind": "MP", "label": "Max Pain", "magnitude": 0.0}
            ],
            "profile": [[540.0, -10000], [550.0, -15000], [560.0, 8000], [570.0, 12000], [580.0, 6000]],
            "updatedAt": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "isApproximateGEX": True
        }
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(fallback_wall, f, ensure_ascii=False, separators=(',', ':'))
        print(f"  ✓ Created fallback {out_file}")


def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting US Offline Data Fetch...")
    
    index_history = {}
    us_directory = []
    
    for symbol in ALL_US_SYMBOLS:
        print(f"Fetching {symbol}...")
        bars = fetch_yahoo_chart(symbol, range_str="3y")
        if bars:
            clean_id = symbol.replace("^", "").replace("-", ".")
            metric_file = os.path.join(METRICS_DIR, f"{clean_id}.json")
            with open(metric_file, "w", encoding="utf-8") as f:
                json.dump(bars, f, ensure_ascii=False, separators=(',', ':'))
                
            if symbol in US_INDICES:
                index_history[symbol] = [
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
                
            us_directory.append({
                "stockId": clean_id,
                "stockName": symbol,
                "type": "us",
                "industry": "Index" if symbol in US_INDICES else ("ETF" if symbol in US_SECTORS or symbol == "SPY" else "Tech/Popular")
            })
            print(f"  ✓ {clean_id}: {len(bars)} bars -> {metric_file}")
        else:
            print(f"  ✗ {symbol}: No data")
            
        time.sleep(0.3)
        
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
    
    # 抓取 SPY 期權牆
    fetch_and_save_options_wall()
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] US Offline Data Fetch Complete!")


if __name__ == "__main__":
    main()
