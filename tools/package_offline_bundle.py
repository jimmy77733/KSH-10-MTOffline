#!/usr/bin/env python3
"""
package_offline_bundle.py
離線資料庫打包器（台美股獨立整合）
- 掃描 dist/offline/metrics 下的所有個股與指數歷史檔（包含台股與美股）
- 產生最新 manifest.json（記錄各標的最新日期與位元組）
- 壓縮生成 mt_offline_bundle.zip（供 App 端一鍵秒級下載）
"""

import json
import os
import sys
import zipfile
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DIST_DIR = os.environ.get("MT_OFFLINE_ROOT", os.path.join(REPO_ROOT, "dist", "offline"))
METRICS_DIR = os.path.join(DIST_DIR, "metrics")


def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Packaging MT Offline Bundle...")
    
    if not os.path.exists(METRICS_DIR):
        print(f"Error: {METRICS_DIR} does not exist.")
        return 1
        
    metric_files = [f for f in os.listdir(METRICS_DIR) if f.endswith(".json")]
    print(f"Found {len(metric_files)} metric files in {METRICS_DIR}")
    
    entries = []
    total_bytes = 0
    
    for filename in sorted(metric_files):
        stock_id = filename[:-5]
        file_path = os.path.join(METRICS_DIR, filename)
        file_bytes = os.path.getsize(file_path)
        total_bytes += file_bytes
        
        latest_date = "1970-01-01"
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list) and data:
                    last_item = data[-1]
                    if isinstance(last_item, dict):
                        latest_date = last_item.get("date", "1970-01-01")
        except Exception:
            pass
            
        entries.append({
            "id": stock_id,
            "latestDate": latest_date,
            "path": f"metrics/{filename}",
            "bytes": file_bytes
        })
        
    now = datetime.now()
    version = now.strftime("%Y%m%d-%H%M")
    
    manifest_data = {
        "version": version,
        "generatedAt": now.isoformat(timespec="seconds") + "Z",
        "totalEntries": len(entries),
        "totalMetricsBytes": total_bytes,
        "bundleZip": "mt_offline_bundle.zip",
        "entries": entries
    }
    
    # 寫入 manifest.json
    manifest_path = os.path.join(DIST_DIR, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, ensure_ascii=False, indent=2)
    print(f"✓ Wrote {manifest_path} ({len(entries)} entries, {total_bytes / 1024 / 1024:.2f} MB uncompressed)")
    
    # 壓縮打包 mt_offline_bundle.zip
    zip_path = os.path.join(DIST_DIR, "mt_offline_bundle.zip")
    print(f"Creating zip bundle: {zip_path}...")
    
    sidecar_files = [
        "manifest.json",
        "assets.json",
        "stock_directory.json",
        "us_index_history.json",
        "us_options_wall.json",
        "us_stock_directory.json",
        "metrics_index.json"
    ]
    
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zipf:
        # 加入根目錄 JSON
        for sidecar in sidecar_files:
            p = os.path.join(DIST_DIR, sidecar)
            if os.path.exists(p):
                zipf.write(p, arcname=sidecar)
                print(f"  + Added {sidecar}")
                
        # 加入 metrics/*.json
        for filename in metric_files:
            p = os.path.join(METRICS_DIR, filename)
            zipf.write(p, arcname=f"metrics/{filename}")
            
    zip_size = os.path.getsize(zip_path)
    print(f"✓ Packaged {zip_path} ({zip_size / 1024 / 1024:.2f} MB compressed)")
    
    # 更新 manifest 內的 zip 大小
    manifest_data["bundleBytes"] = zip_size
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, ensure_ascii=False, indent=2)
        
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Packaging Complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
