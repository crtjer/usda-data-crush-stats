"""Parse NASS QuickStats API response → silver parquet."""

import pandas as pd

from pipeline.config import QUICKSTATS_DIR, SILVER_DIR


def parse_quickstats() -> None:
    """Parse QuickStats CSV data into silver parquet."""
    print("=== Transform: Parsing QuickStats data ===")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = QUICKSTATS_DIR / "grapes_CA.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        df.to_parquet(SILVER_DIR / "quickstats_grapes.parquet", index=False)
        print(f"  quickstats_grapes.parquet: {len(df)} rows")
    else:
        df = pd.DataFrame()
        df.to_parquet(SILVER_DIR / "quickstats_grapes.parquet", index=False)
        print("  No QuickStats data found — creating empty silver parquet")
