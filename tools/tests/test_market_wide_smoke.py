#!/usr/bin/env python3
"""以假 FinMind 回應驗證 market_wide.json 的離線 smoke test。"""

import importlib.util
import json
import os
import tempfile
import unittest
import zipfile
from datetime import date, timedelta


os.environ.setdefault("FINMIND_TOKENS", "smoke-test-token")
SCRIPT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "fetch_historical_data.py",
)
SPEC = importlib.util.spec_from_file_location("fetch_historical_data_smoke", SCRIPT_PATH)
FETCHER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FETCHER)
PACKAGER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "package_offline_bundle.py",
)
PACKAGER_SPEC = importlib.util.spec_from_file_location(
    "package_offline_bundle_smoke", PACKAGER_PATH
)
PACKAGER = importlib.util.module_from_spec(PACKAGER_SPEC)
PACKAGER_SPEC.loader.exec_module(PACKAGER)


class MarketWideSmokeTest(unittest.TestCase):
    """驗證資料契約的欄位、單位分流、排序與保留筆數。"""

    def test_failure_is_recorded_in_status(self):
        def failing_fetch(*_args, **_kwargs):
            raise FETCHER.FetchFailed("假資料來源失敗")

        with tempfile.TemporaryDirectory() as temp_dir:
            original_dist_dir = FETCHER.DIST_DIR
            original_fetch_data = FETCHER.fetch_data
            try:
                FETCHER.DIST_DIR = temp_dir
                FETCHER.fetch_data = failing_fetch
                result = FETCHER.build_market_wide()
                FETCHER.write_market_wide_status(result)
            finally:
                FETCHER.DIST_DIR = original_dist_dir
                FETCHER.fetch_data = original_fetch_data

            with open(os.path.join(temp_dir, "status.json"), encoding="utf-8") as handle:
                status = json.load(handle)
        self.assertFalse(status["marketWide"]["ok"])
        self.assertIn("假資料來源失敗", status["marketWide"]["error"])

    def test_build_market_wide_with_fake_finmind_rows(self):
        dates = [
            (date(2025, 1, 1) + timedelta(days=offset)).isoformat()
            for offset in range(251)
        ]
        latest_date = dates[-1]
        institutional_rows = []
        margin_rows = []
        for day in dates:
            if day == latest_date:
                institutional_rows.extend([
                    {"date": day, "name": "Foreign Investor", "buy": "110", "sell": "10"},
                    {"date": day, "name": "foreign_dealer_self", "buy": "50", "sell": "10"},
                    {"date": day, "name": "Investment_Trust", "buy": "20", "sell": "50"},
                    {"date": day, "name": "Dealer self", "buy": "40", "sell": "10"},
                    {"date": day, "name": "Dealer_Hedging", "buy": "15", "sell": "25"},
                    {"date": day, "name": "Unknown_Fund", "buy": "999", "sell": "0"},
                ])
                margin_purchase_today, margin_purchase_yesterday = "500", "480"
                short_sale_today, short_sale_yesterday = "100", "130"
            else:
                institutional_rows.extend([
                    {"date": day, "name": "Foreign_Investor", "buy": "10", "sell": "3"},
                    {"date": day, "name": "Foreign_Dealer_Self", "buy": "5", "sell": "1"},
                    {"date": day, "name": "Investment Trust", "buy": "4", "sell": "6"},
                    {"date": day, "name": "Dealer_Self", "buy": "3", "sell": "1"},
                    {"date": day, "name": "Dealer Hedging", "buy": "2", "sell": "3"},
                ])
                margin_purchase_today, margin_purchase_yesterday = "200", "190"
                short_sale_today, short_sale_yesterday = "80", "90"
            margin_rows.extend([
                {
                    "date": day,
                    "name": "MarginPurchase",
                    "MarginPurchaseTodayBalance": margin_purchase_today,
                    "MarginPurchaseYesterdayBalance": margin_purchase_yesterday,
                },
                {
                    "date": day,
                    "name": "ShortSale",
                    "ShortSaleTodayBalance": short_sale_today,
                    "ShortSaleYesterdayBalance": short_sale_yesterday,
                },
                {
                    "date": day,
                    "name": "MarginPurchaseMoney",
                    "MarginPurchaseTodayBalance": "999999",
                    "MarginPurchaseYesterdayBalance": "0",
                },
            ])

        price_rows = [
            {
                "date": "2020-01-01",
                "open": "10000",
                "max": "10100",
                "min": "9900",
                "close": "10050",
                "Trading_Volume": "1",
            },
            {
                "date": latest_date,
                "open": "20000",
                "max": "20200",
                "min": "19900",
                "close": "20100",
                "Trading_Volume": "123456",
            },
        ]
        tpex_rows = [{
            "date": latest_date,
            "open": "250",
            "max": "255",
            "min": "248",
            "TPEx": "253",
            "Trading_Volume": "654321",
        }]
        futures_rows = [
            {
                "date": latest_date,
                "contract_date": "2025-10",
                "trading_session": "regular",
                "open": "300",
                "max": "310",
                "min": "290",
                "close": "305",
                "volume": "9999",
            },
            {
                "date": latest_date,
                "contract_date": "2025-09",
                "trading_session": "regular",
                "open": "200",
                "max": "210",
                "min": "190",
                "close": "205",
                "volume": "1",
            },
            {
                "date": latest_date,
                "contract_date": "2025-08",
                "trading_session": "after_market",
                "open": "100",
                "max": "110",
                "min": "90",
                "close": "105",
                "volume": "100000",
            },
        ]
        calls = []

        def fake_fetch(dataset, data_id, start=None, end=None):
            calls.append((dataset, data_id, start, end))
            responses = {
                ("TaiwanStockInstitutionalInvestorsBuySellTotal", None): institutional_rows,
                ("TaiwanStockTotalMarginPurchaseShortSale", None): margin_rows,
                ("TaiwanStockPrice", "TAIEX"): price_rows,
                ("TaiwanStockPrice", "TPEx"): tpex_rows,
                ("TaiwanFuturesDaily", "TX"): futures_rows,
            }
            return responses[(dataset, data_id)]

        with tempfile.TemporaryDirectory() as temp_dir:
            original_dist_dir = FETCHER.DIST_DIR
            original_fetch_data = FETCHER.fetch_data
            try:
                FETCHER.DIST_DIR = temp_dir
                FETCHER.fetch_data = fake_fetch
                result = FETCHER.build_market_wide()
            finally:
                FETCHER.DIST_DIR = original_dist_dir
                FETCHER.fetch_data = original_fetch_data

            self.assertEqual(result, {"ok": True, "asOf": latest_date})
            output_path = os.path.join(temp_dir, "market_wide.json")
            with open(output_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            self.assertFalse([
                name for name in os.listdir(temp_dir)
                if name.startswith(".market_wide.json.") and name.endswith(".tmp")
            ])

        self.assertEqual(len(calls), 5)
        self.assertEqual(
            {call[:2] for call in calls},
            {
                ("TaiwanStockInstitutionalInvestorsBuySellTotal", None),
                ("TaiwanStockTotalMarginPurchaseShortSale", None),
                ("TaiwanStockPrice", "TAIEX"),
                ("TaiwanStockPrice", "TPEx"),
                ("TaiwanFuturesDaily", "TX"),
            },
        )
        self.assertEqual(
            set(payload),
            {"generatedAt", "asOf", "institutional", "margin", "indices"},
        )
        self.assertTrue(payload["generatedAt"].endswith("+08:00"))
        self.assertEqual(payload["asOf"], latest_date)
        self.assertEqual(len(payload["institutional"]), 250)
        self.assertEqual(len(payload["margin"]), 250)
        self.assertEqual(
            [row["date"] for row in payload["institutional"]],
            sorted(row["date"] for row in payload["institutional"]),
        )
        self.assertEqual(
            [row["date"] for row in payload["margin"]],
            sorted(row["date"] for row in payload["margin"]),
        )
        self.assertEqual(
            set(payload["institutional"][-1]),
            {"date", "foreignNet", "trustNet", "dealerNet", "totalNet"},
        )
        self.assertEqual(
            payload["institutional"][-1],
            {
                "date": latest_date,
                "foreignNet": 140.0,
                "trustNet": -30.0,
                "dealerNet": 20.0,
                "totalNet": 130.0,
            },
        )
        self.assertEqual(
            set(payload["margin"][-1]),
            {"date", "marginBalance", "marginDelta", "shortBalance", "shortDelta"},
        )
        self.assertEqual(
            payload["margin"][-1],
            {
                "date": latest_date,
                "marginBalance": 500.0,
                "marginDelta": 20.0,
                "shortBalance": 100.0,
                "shortDelta": -30.0,
            },
        )
        self.assertEqual(set(payload["indices"]), {"TAIEX", "TPEx", "TX"})
        self.assertEqual(len(payload["indices"]["TAIEX"]), 1)
        for rows in payload["indices"].values():
            self.assertEqual(
                [row["date"] for row in rows],
                sorted(row["date"] for row in rows),
            )
            self.assertEqual(
                set(rows[-1]),
                {"date", "open", "high", "low", "close", "volume"},
            )
        self.assertEqual(payload["indices"]["TX"][-1]["close"], 205.0)
        self.assertEqual(payload["indices"]["TX"][-1]["volume"], 1.0)
        print("market_wide.json 假資料片段：")
        print(json.dumps({
            "asOf": payload["asOf"],
            "institutional": payload["institutional"][-1],
            "margin": payload["margin"][-1],
            "TX": payload["indices"]["TX"][-1],
        }, ensure_ascii=False, separators=(",", ":")))


class PackageSidecarSmokeTest(unittest.TestCase):
    """驗證 market_wide 與美股摘要皆會進 manifest 與壓縮檔。"""

    def test_package_includes_all_sidecars_with_hashes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics_dir = os.path.join(temp_dir, "metrics")
            os.makedirs(metrics_dir)
            with open(os.path.join(metrics_dir, "2330.json"), "w", encoding="utf-8") as handle:
                json.dump([{"assetId": "2330", "date": "2025-09-08", "close": 1000}], handle)
            for name in PACKAGER.SIDECAR_FILES:
                with open(os.path.join(temp_dir, name), "w", encoding="utf-8") as handle:
                    json.dump({"sidecar": name}, handle)

            original_dist_dir = PACKAGER.DIST_DIR
            original_metrics_dir = PACKAGER.METRICS_DIR
            try:
                PACKAGER.DIST_DIR = temp_dir
                PACKAGER.METRICS_DIR = metrics_dir
                self.assertEqual(PACKAGER.main(), 0)
            finally:
                PACKAGER.DIST_DIR = original_dist_dir
                PACKAGER.METRICS_DIR = original_metrics_dir

            with open(os.path.join(temp_dir, "manifest.json"), encoding="utf-8") as handle:
                manifest = json.load(handle)
            sidecars = manifest["sidecars"]
            self.assertEqual(
                [entry["name"] for entry in sidecars],
                PACKAGER.SIDECAR_FILES,
            )
            self.assertTrue(all(
                entry["bytes"] > 0 and len(entry["sha256"]) == 64
                for entry in sidecars
            ))
            with zipfile.ZipFile(os.path.join(temp_dir, "mt_offline_bundle.zip")) as archive:
                archived_names = set(archive.namelist())
            self.assertIn("market_wide.json", archived_names)
            self.assertIn("us_metrics_summary.json", archived_names)
            print("sidecar 打包驗證：market_wide.json、us_metrics_summary.json 已列入 manifest 與 zip。")


if __name__ == "__main__":
    unittest.main(verbosity=2)
