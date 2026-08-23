# KSH-10-MTOffline

股帳曆 Market Track **台股離線日線庫**（public）。App WiFi 下載與本機抓取共用此 repo。

## 線上進度（GitHub Pages）

**https://jimmy77733.github.io/KSH-10-MTOffline/**

公開唯讀監控：進度、Actions、資料庫驗證。

## App 下載

```
https://cdn.jsdelivr.net/gh/jimmy77733/KSH-10-MTOffline@main/manifest.json
```

## 本機抓取

```bash
git clone https://github.com/jimmy77733/KSH-10-MTOffline.git
cd KSH-10-MTOffline
cp tools/finmind_tokens.local.example.json tools/finmind_tokens.local.json
# 編輯填入 FinMind token（勿 commit）
./tools/run_fetch_background.sh
./tools/open_fetch_dashboard.sh   # http://127.0.0.1:8765
```

詳見 [`tools/README.md`](tools/README.md)。

## 目錄

```
manifest.json
metrics_index.json
assets.json
stock_directory.json
status.json              # 抓取進度（Pages 顯示）
metrics/{id}.json
tools/                   # 抓取腳本 + 本機儀表板（無 token）
docs/                    # GitHub Pages
```

## GitHub Actions（雲端自動抓取）

Workflow：**Update MT offline data**

- **手動**：Actions → Run workflow
- **排程**：每 6 小時自動續跑（單次 soft budget ~5 小時，結束前會 commit 進度）
- **Secret**：`FINMIND_TOKENS_JSON`（建議，含 name+token）或 `FINMIND_TOKENS`（逗號分隔 JWT）

進度寫回 `metrics/` + `status.json`；App 從 jsDelivr / Pages 下載。全市場靠多次排程疊加跑齊。

### 本機 vs Actions（會不會衝突？）

| | 本機 | GitHub Actions |
|--|------|----------------|
| 腳本 | `scripts/fetch_historical_data.py` | 同檔同步到 `tools/fetch_historical_data.py` |
| 輸出 | `dist/offline/` | 公開 repo 根目錄 `metrics/` 等 |
| Token | `finmind_tokens.local.json` | Secret `FINMIND_TOKENS_JSON` |

**可同時跑，但會搶同一組 FinMind 每小時額度**，更容易集體 402，整體更慢。建議同一時間只留一邊在抓；本機進度要進雲端需再 `git push`／同步。

資料來源：[FinMind](https://finmindtrade.com/)
