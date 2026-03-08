"""Download TTB (Alcohol and Tobacco Tax and Trade Bureau) wine/spirits statistics.

TTB data is difficult to programmatically download, so this module creates
placeholder CSVs with the correct schema when data is unavailable.
"""
import logging
from pathlib import Path

from pipeline.config import TTB_DIR

logger = logging.getLogger(__name__)

WINE_HEADERS = "year,state,wine_type,gallons_produced,gallons_removed,gallons_on_hand"
SPIRITS_HEADERS = "year,spirits_type,proof_gallons_produced,source_material"


def download_ttb(years: list[int] | None = None, force: bool = False) -> None:
    """Download TTB wine/spirits statistics. Creates placeholder if unavailable.

    Args:
        years: Optional list of years to download. None means all.
        force: If True, overwrite existing placeholder files.
    """
    TTB_DIR.mkdir(parents=True, exist_ok=True)

    placeholders = {
        "ttb_wine.csv": WINE_HEADERS,
        "ttb_spirits.csv": SPIRITS_HEADERS,
    }

    for filename, headers in placeholders.items():
        dest = TTB_DIR / filename
        if dest.exists() and not force:
            logger.info("  TTB placeholder already exists: %s", filename)
            continue

        dest.write_text(headers + "\n", encoding="utf-8")
        logger.info("  Created TTB placeholder: %s", filename)

    logger.info(
        "  TTB download is a stub -- manual data acquisition required. "
        "Place CSV files in %s",
        TTB_DIR,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    download_ttb()
