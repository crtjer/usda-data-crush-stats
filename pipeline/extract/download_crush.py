"""Download crush report files from the manifest.

Reads manifest.json and downloads files to data/raw/crush_reports/{year}/.
Supports ZIP extraction (targeting TB08 files) and incremental downloads.
"""
import json
import logging
import re
import zipfile
from io import BytesIO
from pathlib import Path

import requests

from pipeline.config import CRUSH_DIR, RAW_DIR, SESSION_HEADERS

logger = logging.getLogger(__name__)

MANIFEST_PATH = RAW_DIR / "manifest.json"

# Patterns for identifying TB08 files inside ZIPs
TB08_PATTERN = re.compile(r"tb[_]?08", re.IGNORECASE)
SUPPLEMENT_PATTERN = re.compile(r"supp", re.IGNORECASE)


def _extract_tb08_from_zip(zip_bytes: bytes, dest_dir: Path) -> list[Path]:
    """Extract TB08 files from a ZIP archive, skipping supplements.

    Args:
        zip_bytes: Raw bytes of the ZIP file.
        dest_dir: Directory to extract files into.

    Returns:
        List of paths to extracted files.
    """
    extracted = []
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            # Skip directories
            if name.endswith("/"):
                continue
            # Skip supplement files
            if SUPPLEMENT_PATTERN.search(name):
                logger.debug("Skipping supplement: %s", name)
                continue
            # Look for TB08 files
            if TB08_PATTERN.search(name):
                # Flatten path: just use the filename
                filename = Path(name).name
                dest = dest_dir / filename
                dest.write_bytes(zf.read(name))
                extracted.append(dest)
                logger.info("  Extracted: %s", filename)

        # If no TB08 found, extract everything (non-supplement)
        if not extracted:
            logger.warning("No TB08 files found in ZIP; extracting all files")
            for name in zf.namelist():
                if name.endswith("/"):
                    continue
                if SUPPLEMENT_PATTERN.search(name):
                    continue
                filename = Path(name).name
                dest = dest_dir / filename
                dest.write_bytes(zf.read(name))
                extracted.append(dest)
                logger.info("  Extracted (fallback): %s", filename)

    return extracted


def download_crush(years: list[int] | None = None, force: bool = False) -> list[Path]:
    """Download crush report files for specified years.

    Args:
        years: Optional list of years to download. None means all.
        force: If True, re-download even if file already exists.

    Returns:
        List of paths to downloaded/extracted files.
    """
    if not MANIFEST_PATH.exists():
        logger.error("Manifest not found at %s. Run scrape_manifest first.", MANIFEST_PATH)
        return []

    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)

    session = requests.Session()
    session.headers.update(SESSION_HEADERS)

    downloaded = []

    for entry in manifest:
        year = entry["year"]
        if years and year not in years:
            continue

        url = entry["url"]
        filename = entry["filename"]
        fmt = entry["format"]

        year_dir = CRUSH_DIR / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        dest = year_dir / filename

        # Skip if already downloaded (unless forced)
        if not force and dest.exists():
            logger.info("  [%d] Already exists: %s", year, filename)
            downloaded.append(dest)
            continue

        logger.info("  [%d] Downloading: %s", year, filename)
        try:
            resp = session.get(url, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("  [%d] Download failed for %s: %s", year, filename, exc)
            continue

        if fmt == "zip":
            # Save the ZIP itself
            dest.write_bytes(resp.content)
            downloaded.append(dest)
            # Extract TB08 files
            extracted = _extract_tb08_from_zip(resp.content, year_dir)
            downloaded.extend(extracted)
        else:
            dest.write_bytes(resp.content)
            downloaded.append(dest)
            logger.info("  [%d] Saved: %s", year, filename)

    logger.info("Download complete: %d files", len(downloaded))
    return downloaded


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Download NASS crush reports")
    parser.add_argument("--year", type=int, nargs="*", help="Specific years to download")
    parser.add_argument("--force", action="store_true", help="Re-download existing files")
    args = parser.parse_args()

    download_crush(years=args.year, force=args.force)
