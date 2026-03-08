"""Parse acreage PDF reports → silver parquet. Currently a stub."""

import pandas as pd

from pipeline.config import ACREAGE_DIR, SILVER_DIR


def parse_acreage() -> None:
    """Parse acreage PDFs. Currently outputs empty parquet with correct schema."""
    print("=== Transform: Parsing acreage PDFs ===")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # Acreage PDF parsing is complex and not yet implemented
    acreage = pd.DataFrame(columns=[
        "crop_year", "variety_name", "bearing_acres",
        "non_bearing_acres", "total_acres", "county_or_district",
    ])
    acreage.to_parquet(SILVER_DIR / "acreage_raw.parquet", index=False)
    print("  acreage_raw.parquet: 0 rows (PDF parsing not yet implemented)")
