"""Parse TTB wine & spirits files → silver parquet."""

import pandas as pd

from pipeline.config import TTB_DIR, SILVER_DIR


def parse_ttb() -> None:
    """Parse TTB source files into silver parquet."""
    print("=== Transform: Parsing TTB data ===")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # Wine
    wine_files = list(TTB_DIR.glob("wine_*.csv"))
    if wine_files:
        frames = [pd.read_csv(f) for f in wine_files]
        wine = pd.concat(frames, ignore_index=True)
    else:
        wine = pd.DataFrame(columns=[
            "year", "state", "wine_type",
            "gallons_produced", "gallons_removed", "gallons_on_hand",
        ])
        print("  No TTB wine files found — creating empty silver parquet")
    wine.to_parquet(SILVER_DIR / "ttb_wine.parquet", index=False)
    print(f"  ttb_wine.parquet: {len(wine)} rows")

    # Spirits
    spirits_files = list(TTB_DIR.glob("spirits_*.csv"))
    if spirits_files:
        frames = [pd.read_csv(f) for f in spirits_files]
        spirits = pd.concat(frames, ignore_index=True)
    else:
        spirits = pd.DataFrame(columns=[
            "year", "spirits_type",
            "proof_gallons_produced", "source_material",
        ])
        print("  No TTB spirits files found — creating empty silver parquet")
    spirits.to_parquet(SILVER_DIR / "ttb_spirits.parquet", index=False)
    print(f"  ttb_spirits.parquet: {len(spirits)} rows")
