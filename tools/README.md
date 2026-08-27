# MT 離線抓取工具

本機執行 FinMind 全市場抓取、監控儀表板。**不含 token**，請自行建立 `finmind_tokens.local.json`。

## 快速開始

```bash
cp finmind_tokens.local.example.json finmind_tokens.local.json
# 編輯 JSON，填入 FinMind「API token 金鑰」（可多組）

./run_fetch_background.sh          # 背景抓取 → 寫入 repo 根目錄
./open_fetch_dashboard.sh          # 本機儀表板 http://127.0.0.1:8765
```

## 檔案

| 檔案 | 用途 |
|------|------|
| `fetch_historical_data.py` | 全市場抓取、驗證、產出 manifest |
| `fetch_status_server.py` + `fetch_dashboard.html` | 本機監控（Token 402/403、啟停） |
| `finmind_tokens.local.json` | **gitignore**，你的 token |
| `fetch_run.log` / `fetch.pid` | 執行紀錄 |

## 環境變數

- `MT_OFFLINE_ROOT`：資料輸出目錄（預設 = repo 根目錄）
- `FINMIND_TOKENS`：逗號分隔 token（可取代 json 檔）

## GitHub Pages vs 本機

| 功能 | [GitHub Pages](../) | 本機儀表板 |
|------|---------------------|------------|
| 看完成進度 | ✅ 讀 `status.json` | ✅ |
| Token 402/403 檢測 | ❌ | ✅ |
| 啟動／停止抓取 | ❌ | ✅ |

Pages 只能**唯讀**看已 push 的 `status.json`；抓取必須在本機或 CI 跑。

## GitHub Actions（可選）

`.github/workflows/fetch-offline-data.yml` 支援手動觸發與排程自動執行:

- **Secrets**: 必須設定 `FINMIND_TOKENS_JSON` (建議，JSON 陣列) 或 `FINMIND_TOKENS` (逗號分隔)
- **排程時間**:
  - 每 8 小時續跑（UTC 20分）
  - 台股盤後：台北 **15:30** (UTC 07:30 週一~五) — 確保 TWSE 結算完成
  - 美股收盤：美東 16:30 (UTC 20:30 週一~五)

⚠️ 台股 cron 必須在結算後執行 (約 14:30-15:00 完成),過早抓取會拿到空 EOD。

注意：首次約 3000 檔需數十小時，**不建議**在 Actions 跑完整初次抓取（6 小時上限）；適合之後增量更新。
