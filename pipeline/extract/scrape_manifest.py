"""Build a manifest of all downloadable NASS crush report files.

Crawls the NASS website and combines with hardcoded known URLs to produce
a deduplicated manifest saved to data/raw/manifest.json.
"""
import json
import logging
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from pipeline.config import (
    CRUSH_DIR,
    NASS_BASE,
    NASS_ERRATA,
    NASS_FINAL,
    RAW_DIR,
    SESSION_HEADERS,
)

logger = logging.getLogger(__name__)

NASS_INDEX_URL = (
    "https://www.nass.usda.gov/Statistics_by_State/California/"
    "Publications/Specialty_and_Other_Releases/Grapes/Crush/Reports/index.php"
)

# ---------------------------------------------------------------------------
# Hardcoded known URLs (primary source -- NASS page structure is unreliable)
# ---------------------------------------------------------------------------

ERRATA_CSV_URLS = {
    2024: f"{NASS_ERRATA}/2024/2024_errata_gc_tb08.csv",
    2023: f"{NASS_ERRATA}/2023/2023_errata_gc_tb08.csv",
    2022: f"{NASS_ERRATA}/2022/2022_errata_gcbtb08_WEB_B.csv",
}

KNOWN_CSV_URLS = {
    2024: f"{NASS_FINAL}/2024/2024_final_gcbtb08.csv",
    2023: f"{NASS_FINAL}/2023/2023_final_gc_tb08_web_b.csv",
    2022: f"{NASS_FINAL}/2022/Grape_Crush_2022_Final_gcbtb08_WEB_B.csv",
}

KNOWN_ZIP_URLS = {
    2021: f"{NASS_FINAL}/2021/gc_2021_final.zip",
    2020: f"{NASS_FINAL}/2020/2020_gcb_xls_final.zip",
    2019: f"{NASS_FINAL}/2019/2019gcbxls.zip",
    2018: f"{NASS_FINAL}/2018/2018gcbxls.zip",
    2017: f"{NASS_FINAL}/2017/201703gcbxls.zip",
    2016: f"{NASS_FINAL}/2016/201603gcbxls.zip",
    2015: f"{NASS_FINAL}/2015/201503gcbxls.zip",
    2014: f"{NASS_FINAL}/2014/201403gcbxls.zip",
    2013: f"{NASS_FINAL}/2013/201303gcbxls.zip",
    2012: f"{NASS_FINAL}/2012/201203gcbxls.zip",
    2011: f"{NASS_FINAL}/2011/201103gcbxls.zip",
    2010: f"{NASS_FINAL}/2010/201003gcbxls.zip",
    2009: f"{NASS_FINAL}/2009/200903gcbxls.zip",
    2008: f"{NASS_FINAL}/2008/200803gcbxls.zip",
    2007: f"{NASS_FINAL}/2007/200703gcbxls.zip",
    2006: f"{NASS_FINAL}/2006/200603gcbxls.zip",
    2005: f"{NASS_FINAL}/2005/200503gcbxls.zip",
    2004: f"{NASS_FINAL}/2004/200403gcbxls.zip",
    2003: f"{NASS_FINAL}/2003/200303gcbxls.zip",
    2002: f"{NASS_FINAL}/2002/200203gcbxls.zip",
    2001: f"{NASS_FINAL}/2001/200109gcbxls.zip",
    2000: f"{NASS_FINAL}/2000/200003gcbxls.zip",
}

ERRATA_ZIP_URLS = {
    2021: f"{NASS_ERRATA}/2021/2021_10_grape_crush_errata.zip",
    2020: f"{NASS_ERRATA}/2020/gc_2020_errata_xls.zip",
    2019: f"{NASS_ERRATA}/2019/2019erratagcbxls.zip",
    2018: f"{NASS_ERRATA}/2018/2018errata.gcbxls.zip",
    2017: f"{NASS_ERRATA}/2017/2017erratagcbtb.zip",
    2016: f"{NASS_ERRATA}/2016/201708errataxls.zip",
    2015: f"{NASS_ERRATA}/2015/201608errataxls.zip",
    2014: f"{NASS_ERRATA}/2014/201507errataxls.zip",
    2013: f"{NASS_ERRATA}/2013/201406errataxls.zip",
    2012: f"{NASS_ERRATA}/2012/201208gcbxls.zip",
    2011: f"{NASS_ERRATA}/2011/201107errataxls.zip",
}


def _make_entry(year: int, report_type: str, url: str) -> dict:
    """Create a single manifest entry dict."""
    filename = url.rsplit("/", 1)[-1]
    fmt = filename.rsplit(".", 1)[-1].lower()
    # Attempt to extract table name from filename
    table = "tb08" if "tb08" in filename.lower() or "tb_08" in filename.lower() else ""
    return {
        "year": year,
        "report_type": report_type,
        "table": table,
        "url": url,
        "filename": filename,
        "format": fmt,
    }


def _hardcoded_entries(years: list[int] | None = None) -> list[dict]:
    """Build manifest entries from hardcoded URL dictionaries."""
    entries = []

    sources = [
        (KNOWN_CSV_URLS, "Final"),
        (KNOWN_ZIP_URLS, "Final"),
        (ERRATA_CSV_URLS, "Errata"),
        (ERRATA_ZIP_URLS, "Errata"),
    ]

    for url_map, report_type in sources:
        for year, url in url_map.items():
            if years and year not in years:
                continue
            entries.append(_make_entry(year, report_type, url))

    return entries


def _parse_year_from_url(url: str) -> int | None:
    """Try to extract a four-digit year from the URL path."""
    match = re.search(r"/(\d{4})/", url)
    if match:
        yr = int(match.group(1))
        if 1990 <= yr <= 2099:
            return yr
    return None


def _parse_report_type(url: str) -> str:
    """Determine report type from URL path segments."""
    url_lower = url.lower()
    if "/errata/" in url_lower:
        return "Errata"
    if "/prelim/" in url_lower:
        return "Prelim"
    return "Final"


def _scrape_nass_index(years: list[int] | None = None) -> list[dict]:
    """Attempt to scrape the NASS index page for additional report links."""
    entries = []
    try:
        session = requests.Session()
        session.headers.update(SESSION_HEADERS)
        resp = session.get(NASS_INDEX_URL, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        link_pattern = re.compile(r"\.(csv|zip|pdf)$", re.IGNORECASE)

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if not link_pattern.search(href):
                continue

            abs_url = urljoin(NASS_INDEX_URL, href)

            # Only include links under the NASS crush base
            if NASS_BASE not in abs_url:
                continue

            year = _parse_year_from_url(abs_url)
            if year is None:
                continue
            if years and year not in years:
                continue

            report_type = _parse_report_type(abs_url)
            entries.append(_make_entry(year, report_type, abs_url))

    except requests.RequestException as exc:
        logger.warning("Could not scrape NASS index page: %s", exc)

    return entries


def build_manifest(years: list[int] | None = None) -> list[dict]:
    """Build a deduplicated manifest of crush report files.

    Priority: errata > final. Hardcoded URLs are the primary source;
    scraped links supplement them.

    Args:
        years: Optional list of years to include. None means all.

    Returns:
        List of manifest entry dicts.
    """
    # Start with hardcoded URLs (reliable)
    entries = _hardcoded_entries(years)

    # Try to scrape for additional links
    scraped = _scrape_nass_index(years)
    entries.extend(scraped)

    # Deduplicate: key by (year, format), prefer Errata > Final > Prelim
    priority = {"Errata": 0, "Final": 1, "Prelim": 2}
    best: dict[tuple[int, str], dict] = {}

    for entry in entries:
        key = (entry["year"], entry["format"])
        existing = best.get(key)
        if existing is None:
            best[key] = entry
        else:
            # Keep the higher-priority report type
            if priority.get(entry["report_type"], 99) < priority.get(
                existing["report_type"], 99
            ):
                best[key] = entry

    manifest = sorted(best.values(), key=lambda e: (-e["year"], e["format"]))

    # Save manifest
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = RAW_DIR / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    logger.info("Manifest saved to %s (%d entries)", manifest_path, len(manifest))

    return manifest


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manifest = build_manifest()
    print(f"Built manifest with {len(manifest)} entries")
    for entry in manifest[:5]:
        print(f"  {entry['year']} {entry['report_type']:8s} {entry['filename']}")
