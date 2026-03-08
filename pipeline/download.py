"""Bronze layer: download raw USDA NASS Grape Crush TB08 files."""

import io
import json
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze" / "grape_crush"
LOG_FILE = BASE_DIR / "data" / "bronze" / "download_log.jsonl"

NASS_BASE = "https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Crush"
NASS_FINAL = f"{NASS_BASE}/Final"
NASS_ERRATA = f"{NASS_BASE}/Errata"

# Errata TB08 CSVs take priority (most current corrections)
ERRATA_CSV_URLS = {
    2024: f"{NASS_ERRATA}/2024/2024_errata_gc_tb08.csv",
    2023: f"{NASS_ERRATA}/2023/2023_errata_gc_tb08.csv",
    2022: f"{NASS_ERRATA}/2022/2022_errata_gcbtb08_WEB_B.csv",
}

# Known direct CSV URLs by year (Final versions)
KNOWN_CSV_URLS = {
    2024: f"{NASS_FINAL}/2024/2024_final_gcbtb08.csv",
    2023: f"{NASS_FINAL}/2023/2023_final_gc_tb08_web_b.csv",
    2022: f"{NASS_FINAL}/2022/Grape_Crush_2022_Final_gcbtb08_WEB_B.csv",
}

# Known ZIP URLs (contain XLS/XLSX with TB08 sheet) — corrected from NASS index
KNOWN_ZIP_URLS = {
    2021: f"{NASS_FINAL}/2021/gc_2021_final.zip",
    2020: f"{NASS_FINAL}/2020/2020_gcb_xls_final.zip",
    2019: f"{NASS_FINAL}/2019/2019gcbxls.zip",
    2018: f"{NASS_FINAL}/2018/2018gcbxls.zip",
    2017: f"{NASS_FINAL}/2017/201703gcbxls.zip",
    2016: f"{NASS_FINAL}/2016/201603gcbxls.zip",
}

# Errata ZIPs (prefer over Final when available)
ERRATA_ZIP_URLS = {
    2021: f"{NASS_ERRATA}/2021/2021_10_grape_crush_errata.zip",
    2020: f"{NASS_ERRATA}/2020/gc_2020_errata_xls.zip",
    2019: f"{NASS_ERRATA}/2019/2019erratagcbxls.zip",
    2018: f"{NASS_ERRATA}/2018/2018errata.gcbxls.zip",
    2017: f"{NASS_ERRATA}/2017/2017erratagcbtb.zip",
    2016: f"{NASS_ERRATA}/2016/201708errataxls.zip",
}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "USDA-Data-Pipeline/1.0 (research; crtjer@gmail.com)"
})


def _log_download(year: int, filename: str, url: str, status: str, size: int = 0):
    """Append a download event to the JSONL log."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "year": year,
        "filename": filename,
        "source_url": url,
        "status": status,
        "size_bytes": size,
    }
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


def _download_file(url: str, dest: Path, year: int) -> bool:
    """Download a file. Returns True on success."""
    try:
        resp = SESSION.get(url, timeout=60)
        resp.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(resp.content)
        _log_download(year, dest.name, url, "ok", len(resp.content))
        print(f"  [{year}] Downloaded {dest.name} ({len(resp.content):,} bytes)")
        return True
    except requests.RequestException as e:
        _log_download(year, dest.name, url, f"error: {e}", 0)
        print(f"  [{year}] FAILED {url}: {e}")
        return False


def _extract_tb08_from_zip(zip_path: Path, year: int) -> bool:
    """Extract TB08 XLSX or CSV from a ZIP file."""
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            # Find TB08 file (XLSX or CSV)
            tb08_file = None
            # First pass: exact tb08 match, exclude supplements
            for name in names:
                lower = name.lower()
                if "supplement" in lower:
                    continue
                if "tb08" in lower or "tb_08" in lower:
                    tb08_file = name
                    break
            if not tb08_file:
                # Second pass: broader "08" match, exclude supplements
                for name in names:
                    lower = name.lower()
                    if "supplement" in lower:
                        continue
                    if lower.endswith((".xlsx", ".xls", ".csv")) and ("08" in lower or "table8" in lower or "table_8" in lower):
                        tb08_file = name
                        break
            if tb08_file:
                dest_dir = zip_path.parent
                zf.extract(tb08_file, dest_dir)
                extracted = dest_dir / tb08_file
                print(f"  [{year}] Extracted {tb08_file} from ZIP")
                return True
            else:
                # Extract all files - we'll find what we need later
                zf.extractall(zip_path.parent)
                print(f"  [{year}] No TB08 found in ZIP, extracted all: {names}")
                return True
    except zipfile.BadZipFile as e:
        print(f"  [{year}] Bad ZIP file: {e}")
        return False


def _try_scrape_index(year: int) -> list[str]:
    """Try to find download URLs by scraping the NASS index page for a year."""
    index_url = f"{NASS_BASE}/{year}/"
    try:
        resp = SESSION.get(index_url, timeout=30)
        if resp.status_code != 200:
            return []
        # Simple link extraction
        import re
        links = re.findall(r'href=["\']([^"\']+)["\']', resp.text, re.IGNORECASE)
        results = []
        for link in links:
            lower = link.lower()
            if lower.endswith((".csv", ".zip", ".xlsx")):
                if link.startswith("http"):
                    results.append(link)
                elif link.startswith("/"):
                    results.append(f"https://www.nass.usda.gov{link}")
                else:
                    results.append(f"{NASS_BASE}/{year}/{link}")
        return results
    except Exception:
        return []


def download_year(year: int, force: bool = False) -> bool:
    """Download TB08 data for a given crop year. Returns True if data available."""
    year_dir = BRONZE_DIR / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    # Check if we already have data for this year
    existing = (list(year_dir.glob("*.csv")) + list(year_dir.glob("**/*.csv"))
                + list(year_dir.glob("*.xlsx")) + list(year_dir.glob("**/*.xlsx"))
                + list(year_dir.glob("*.xls")) + list(year_dir.glob("**/*.xls")))
    if existing and not force:
        print(f"  [{year}] Already have data, skipping (use --force to re-download)")
        return True

    # Priority 1: Errata CSV (most current corrections)
    if year in ERRATA_CSV_URLS:
        csv_url = ERRATA_CSV_URLS[year]
        csv_name = csv_url.split("/")[-1]
        if _download_file(csv_url, year_dir / csv_name, year):
            return True

    # Priority 2: Final CSV
    if year in KNOWN_CSV_URLS:
        csv_url = KNOWN_CSV_URLS[year]
        csv_name = csv_url.split("/")[-1]
        if _download_file(csv_url, year_dir / csv_name, year):
            return True

    # Priority 3: Errata ZIP
    if year in ERRATA_ZIP_URLS:
        zip_url = ERRATA_ZIP_URLS[year]
        zip_name = zip_url.split("/")[-1]
        zip_dest = year_dir / zip_name
        if _download_file(zip_url, zip_dest, year):
            _extract_tb08_from_zip(zip_dest, year)
            return True

    # Priority 4: Final ZIP
    if year in KNOWN_ZIP_URLS:
        zip_url = KNOWN_ZIP_URLS[year]
        zip_name = zip_url.split("/")[-1]
        zip_dest = year_dir / zip_name
        if _download_file(zip_url, zip_dest, year):
            _extract_tb08_from_zip(zip_dest, year)
            return True

    print(f"  [{year}] No data source found")
    return False


def download_all(years: list[int], force: bool = False) -> dict:
    """Download TB08 data for all specified years."""
    print("=== Bronze Layer: Downloading raw data ===")
    results = {}
    for year in sorted(years):
        results[year] = download_year(year, force=force)
    success = sum(1 for v in results.values() if v)
    print(f"\nDownload complete: {success}/{len(years)} years acquired")
    return results


if __name__ == "__main__":
    import sys
    years = list(range(2016, 2025))
    force = "--force" in sys.argv
    download_all(years, force=force)
