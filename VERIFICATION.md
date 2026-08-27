# Taiwan Stock EOD Refresh Fix - Verification Guide

## Problem Summary

**Before**: Manifest version showed `20260827-0756` but all `latestDate` values were stuck at `2026-08-21`, missing 3 trading days (8/24, 8/25, 8/26).

**Root Cause**: GitHub Actions cron for Taiwan stocks ran **before TWSE settlement completed**.

## Fix Applied

### Cron Schedule Changes

| Schedule | UTC Time | Taipei Time | Status | Notes |
|----------|----------|-------------|--------|-------|
| Old #1 | 00:30 Mon-Fri | 08:30 | ❌ Pre-open | No EOD data available |
| Old #2 | 05:30 Mon-Fri | 13:30 | ❌ Intraday | Settlement not complete |
| **New** | **07:30 Mon-Fri** | **15:30** | ✅ Post-settlement | TWSE EOD finalized ~14:30-15:00 |
| US (unchanged) | 20:30 Mon-Fri | 04:30+1 | ✅ | US market close 16:30 ET |

### Additional Fixes

1. **Error Handling**: Token missing now fails explicitly (`exit 1`) instead of silent skip
2. **Documentation**: Clear timezone explanation in README and workflow comments
3. **Workflow Logic**: Updated schedule references in RUN_TW/RUN_US env vars

## Why This Fixes the Stale Data Issue

### Old Behavior (Broken)
1. Cron fires at TPE 08:30 or 13:30
2. TWSE settlement not complete → FinMind API returns empty array
3. Python script's `incremental_start_date` sees no new data → `latestDate` unchanged
4. Manifest version advances (timestamp) but actual data stays stale

### New Behavior (Fixed)
1. Cron fires at TPE 15:30 (after 14:30-15:00 settlement window)
2. TWSE settlement complete → FinMind API returns current day EOD
3. Python script merges new bars → `latestDate` advances to today
4. Manifest version and data stay synchronized

## Verification Steps (Post-Merge)

**You cannot verify this immediately** — must wait for next Taiwan trading day's cron run.

### On Next Weekday After Merge

1. Check Actions tab for workflow run triggered at ~07:30 UTC (15:30 TPE)
2. After completion, verify:
   ```bash
   # Check manifest version
   curl -s https://cdn.jsdelivr.net/gh/jimmy77733/KSH-10-MTOffline@main/manifest.json | jq '.version, .entries[0:3]'
   
   # Sample a few tickers
   curl -s https://cdn.jsdelivr.net/gh/jimmy77733/KSH-10-MTOffline@main/metrics/2330.json | jq '.[-1].date'
   curl -s https://cdn.jsdelivr.net/gh/jimmy77733/KSH-10-MTOffline@main/metrics/0050.json | jq '.[-1].date'
   ```
3. Expected: `latestDate` should be the current trading day (e.g., 2026-08-27 if run on 8/27)

### Compare With Status.json

```bash
curl -s https://cdn.jsdelivr.net/gh/jimmy77733/KSH-10-MTOffline@main/status.json | jq '{calendarLast, updatedAt, round, coveragePercent}'
```

- `calendarLast` should advance to current day
- `updatedAt` should be ~15:30-19:00 TPE (depending on fetch duration)

## Why Old Cron Times Were Wrong

**Taiwan Stock Exchange EOD Data Flow:**
1. Market closes: 13:30
2. Settlement processing: 13:30-14:30
3. **Official EOD finalized: ~14:30-15:00** ← FinMind data becomes available
4. Data providers (FinMind) update APIs: 14:30-15:30

**Old cron execution:**
- 08:30 TPE: Market hasn't even opened yet
- 13:30 TPE: Market just closed, settlement in progress

Both times are **before step 3 completes** → API returns empty data for "today".

## No Backfill Needed

The next successful cron run will automatically fetch missing days via `incremental_start_date` logic:
- Script loads existing metrics, finds `latestDate = 2026-08-21`
- Calculates `start_date = 2026-08-22` (next day)
- Fetches 2026-08-22 through today in one request
- Gap fills automatically

## Testing Locally (Optional)

To test the timing logic without waiting for cron:

```bash
# Ensure you have FinMind tokens configured
cp tools/finmind_tokens.local.example.json tools/finmind_tokens.local.json
# Edit and add your token

# Run fetch manually (will fetch up to today if after 15:00 TPE)
python3 tools/fetch_historical_data.py

# Check a sample ticker
python3 -c "import json; d=json.load(open('metrics/2330.json')); print('Latest:', d[-1]['date'])"
```

If run after 15:00 TPE on a trading day, `latestDate` should be today. If run before settlement, it will be yesterday.

## Summary

- ✅ Taiwan cron moved from pre-settlement (08:30/13:30) to post-settlement (15:30 TPE)
- ✅ Token errors now fail loudly instead of silent skip
- ✅ Documentation updated with timezone explanations
- ✅ No code changes to fetch logic needed — only schedule timing
- ✅ Next cron run will automatically backfill missing days

**Result**: Users downloading "latest" pack will get actual latest settled trading day, not stale data from last week.
