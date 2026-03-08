"""Download data from the NASS QuickStats API.

Requires a NASS API key set in the NASS_API_KEY environment variable.
Request a key at https://quickstats.nass.usda.gov/api
"""
import json
import logging
import os

import requests

from pipeline.config import QUICKSTATS_DIR, SESSION_HEADERS

logger = logging.getLogger(__name__)

QUICKSTATS_API_URL = "https://quickstats.nass.usda.gov/api/api_GET/"


def download_quickstats(years: list[int] | None = None) -> None:
    """Download NASS QuickStats data. Requires NASS_API_KEY env var.

    Args:
        years: Optional list of years to query.
    """
    api_key = os.environ.get("NASS_API_KEY")
    if not api_key:
        print("  NASS_API_KEY not set, skipping QuickStats download")
        return

    QUICKSTATS_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(SESSION_HEADERS)

    params = {
        "key": api_key,
        "commodity_desc": "GRAPES",
        "state_alpha": "CA",
        "format": "JSON",
    }

    if years:
        for year in years:
            params["year"] = str(year)
            _fetch_quickstats(session, params, year)
    else:
        _fetch_quickstats(session, params, year=None)


def _fetch_quickstats(
    session: requests.Session, params: dict, year: int | None
) -> None:
    """Fetch a single QuickStats query and save results."""
    label = str(year) if year else "all_years"
    dest = QUICKSTATS_DIR / f"quickstats_{label}.json"

    if dest.exists():
        logger.info("  QuickStats data already exists: %s", dest.name)
        return

    logger.info("  Fetching QuickStats data for %s...", label)
    try:
        resp = session.get(QUICKSTATS_API_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        logger.error("  QuickStats request failed: %s", exc)
        return

    with open(dest, "w") as f:
        json.dump(data, f, indent=2)
    logger.info("  Saved QuickStats data: %s", dest.name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    download_quickstats()
