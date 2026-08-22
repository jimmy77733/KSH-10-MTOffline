# KSH-10-MTOffline

股帳曆（StockCalendar）Market Track **台股離線日線庫**。此 repo 為 **public**，App 與本機都可以免登入下載。

對應 App：[`jimmy77733/KSH-10-StockCalendar`](https://github.com/jimmy77733/KSH-10-StockCalendar)（私人）。完整庫不打進 IPA。

## 目錄

```
manifest.json          # App 下載清單
metrics_index.json     # 排行榜用摘要
assets.json
stock_directory.json
metrics/{id}.json      # 每標的約 5 年日線（OHLC／量／法人／當沖／融資）
```

## App 下載

Manifest（jsDelivr）：

`https://cdn.jsdelivr.net/gh/jimmy77733/KSH-10-MTOffline@main/manifest.json`

設定 → Market Track 離線資料 → 建議 Wi‑Fi。

## 本機整包

見 [Releases](https://github.com/jimmy77733/KSH-10-MTOffline/releases) 的 `tw-offline-full.zip`。

資料來自 [FinMind](https://finmindtrade.com/)。
