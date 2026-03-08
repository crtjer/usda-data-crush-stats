"""Download grape acreage report PDFs from NASS.

Acreage reports are PDF-only and require specialized parsing.
This module is currently a stub.
"""
import logging

from pipeline.config import ACREAGE_DIR

logger = logging.getLogger(__name__)


def download_acreage(years: list[int] | None = None, force: bool = False) -> None:
    """Download grape acreage report PDFs. Currently a stub.

    Args:
        years: Optional list of years to download.
        force: If True, re-download existing files.
    """
    print("  Acreage PDF download not yet implemented (PDF-only source)")

    ACREAGE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(
        "  Acreage directory ready at %s -- place PDF files here manually.",
        ACREAGE_DIR,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    download_acreage()
