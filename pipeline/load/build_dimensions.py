"""Build dimension CSV tables for the Gold layer."""
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import SILVER_DIR, FINAL_DIR, GRAPE_TYPE_MAP

DISTRICT_DATA = [
    (0, "State Total", "Statewide", "Sum of all districts (alias for district 18)"),
    (1, "Mendocino", "North Coast", "Mendocino County"),
    (2, "Lake", "North Coast", "Lake County"),
    (3, "Sonoma/Marin", "North Coast", "Sonoma and Marin Counties"),
    (4, "Napa", "North Coast", "Napa County"),
    (5, "Solano", "North Coast/Central", "Solano County"),
    (6, "Contra Costa/Alameda", "Central Coast", "Contra Costa and Alameda Counties"),
    (7, "San Joaquin", "Central Valley", "San Joaquin County"),
    (8, "Sacramento/Yolo", "Sacramento Valley", "Sacramento and Yolo Counties"),
    (9, "El Dorado/Amador", "Sierra Foothills", "El Dorado and Amador Counties"),
    (10, "San Benito/Santa Clara/Santa Cruz", "Central Coast", "San Benito, Santa Clara, and Santa Cruz Counties"),
    (11, "Monterey", "Central Coast", "Monterey County"),
    (12, "San Luis Obispo/Santa Barbara/Ventura", "South Coast", "San Luis Obispo, Santa Barbara, and Ventura Counties"),
    (13, "Southern California", "Southern California", "Los Angeles, Riverside, San Bernardino, San Diego Counties"),
    (14, "Stanislaus/Merced/Mariposa", "Central Valley", "Stanislaus, Merced, and Mariposa Counties"),
    (15, "Madera", "Central Valley", "Madera County"),
    (16, "Fresno", "Central Valley", "Fresno County"),
    (17, "Tulare/Kings/Kern", "Central Valley", "Tulare, Kings, and Kern Counties"),
    (18, "State Total", "Statewide", "State total (all districts combined)"),
]

GRAPE_CATEGORY_MAP = {
    4: "raisin",
    5: "table",
    6: "wine",
    7: "wine",
    8: "wine",
}


def _build_dim_district() -> pd.DataFrame:
    """Build static district dimension table."""
    df = pd.DataFrame(
        DISTRICT_DATA,
        columns=["district_id", "district_name", "region", "description"],
    )
    return df


def _build_dim_grape_variety(years: list[int]) -> pd.DataFrame:
    """Build grape variety dimension from silver parquet files."""
    frames = []
    for year in years:
        path = SILVER_DIR / f"{year}_tb08.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            frames.append(df)

    if not frames:
        print("WARNING: No silver parquet files found for dim_grape_variety.")
        return pd.DataFrame(
            columns=[
                "variety_code",
                "variety_name",
                "grape_type_code",
                "grape_type_name",
                "grape_category",
            ]
        )

    combined = pd.concat(frames, ignore_index=True)

    # Extract unique variety information
    variety_cols = ["variety_code", "variety_name", "grape_type_code", "grape_type_name"]
    # Keep only columns that exist
    existing_cols = [c for c in variety_cols if c in combined.columns]
    if not existing_cols or "variety_code" not in existing_cols:
        print("WARNING: Required variety columns not found in silver data.")
        return pd.DataFrame(
            columns=[
                "variety_code",
                "variety_name",
                "grape_type_code",
                "grape_type_name",
                "grape_category",
            ]
        )

    varieties = combined[existing_cols].drop_duplicates(subset=["variety_code"])

    # Add grape_category from grape_type_code
    if "grape_type_code" in varieties.columns:
        varieties["grape_category"] = (
            varieties["grape_type_code"].map(GRAPE_CATEGORY_MAP).fillna("unknown")
        )
    else:
        varieties["grape_category"] = "unknown"

    # Ensure all expected columns present
    for col in ["variety_name", "grape_type_code", "grape_type_name", "grape_category"]:
        if col not in varieties.columns:
            varieties[col] = None

    varieties = varieties.sort_values("variety_code").reset_index(drop=True)
    return varieties[
        [
            "variety_code",
            "variety_name",
            "grape_type_code",
            "grape_type_name",
            "grape_category",
        ]
    ]


def _build_dim_crop_year(years: list[int]) -> pd.DataFrame:
    """Build crop year dimension from silver parquet files."""
    rows = []
    now = datetime.now(timezone.utc).isoformat()

    for year in years:
        path = SILVER_DIR / f"{year}_tb08.parquet"
        if path.exists():
            rows.append(
                {
                    "crop_year": year,
                    "report_type": "Final",
                    "source_url": (
                        f"https://www.nass.usda.gov/Statistics_by_State/California/"
                        f"Publications/Specialty_and_Other_Releases/Grapes/Crush/Final/"
                        f"{year}/"
                    ),
                    "first_ingested_at": now,
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=["crop_year", "report_type", "source_url", "first_ingested_at"]
        )

    df = pd.DataFrame(rows)
    df = df.sort_values("crop_year").reset_index(drop=True)
    return df


def build_dimensions(years: list[int]) -> None:
    """Build all dimension CSV tables and write to data/final/."""
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    # dim_district
    dim_district = _build_dim_district()
    dim_district.to_csv(FINAL_DIR / "dim_district.csv", index=False)
    print(f"dim_district.csv: {len(dim_district)} rows")

    # dim_grape_variety
    dim_variety = _build_dim_grape_variety(years)
    dim_variety.to_csv(FINAL_DIR / "dim_grape_variety.csv", index=False)
    print(f"dim_grape_variety.csv: {len(dim_variety)} rows")

    # dim_crop_year
    dim_year = _build_dim_crop_year(years)
    dim_year.to_csv(FINAL_DIR / "dim_crop_year.csv", index=False)
    print(f"dim_crop_year.csv: {len(dim_year)} rows")

    print("Dimensions built successfully.")
