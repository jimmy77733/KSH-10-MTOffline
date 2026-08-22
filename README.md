# KSH-10-MTOffline

股帳曆 Market Track **台股離線日線庫**（public）。App WiFi 下載與本機抓取共用此 repo。

## 線上進度（GitHub Pages）

**https://jimmy77733.github.io/KSH-10-MTOffline/**

唯讀檢視 `status.json` / `manifest.json` 進度（約 30 秒刷新）。

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

## GitHub Actions

可选手動執行 **Update MT offline data**（需 repo Secret `FINMIND_TOKENS`）。  
初次 3000+ 檔建議**本機**跑完再 push；Actions 有 6 小時上限。

資料來源：[FinMind](https://finmindtrade.com/)
