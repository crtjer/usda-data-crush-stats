"""Silver layer: build dimension tables from all parsed years."""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"

# USDA California Grape Pricing Districts (hardcoded from USDA spec)
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


def build_dim_district() -> pd.DataFrame:
    """Build district dimension table (static)."""
    df = pd.DataFrame(DISTRICT_DATA, columns=["district_id", "district_name", "region", "description"])
    return df


def build_dim_grape_type(all_data: pd.DataFrame) -> pd.DataFrame:
    """Build grape type dimension table from parsed data."""
    type_map = {
        4: ("Raisin Grapes", "raisin"),
        5: ("Table Grapes", "table"),
        6: ("Wine Grapes (Red)", "wine"),
        7: ("Wine Grapes (White)", "wine"),
    }

    rows = []
    for code, (name, category) in type_map.items():
        rows.append({"grape_type_code": code, "grape_type_name": name, "category": category})

    # Also pull any types from actual data not in our map
    if "grape_type_code" in all_data.columns:
        for code in all_data["grape_type_code"].dropna().unique():
            code = int(code)
            if code not in type_map:
                name_vals = all_data.loc[all_data["grape_type_code"] == code, "grape_type_name"].dropna().unique()
                name = name_vals[0] if len(name_vals) > 0 else f"Unknown ({code})"
                rows.append({"grape_type_code": code, "grape_type_name": str(name), "category": "unknown"})

    return pd.DataFrame(rows)


def build_dim_grape_variety(all_data: pd.DataFrame) -> pd.DataFrame:
    """Build grape variety dimension table from all years of data."""
    if all_data.empty:
        return pd.DataFrame(columns=["variety_code", "variety_name", "grape_type_code", "grape_type_name", "grape_category"])

    # Get unique variety_code + variety_name + grape_type_code combos
    cols = ["variety_code", "variety_name", "grape_type_code", "grape_type_name"]
    available_cols = [c for c in cols if c in all_data.columns]
    varieties = all_data[available_cols].drop_duplicates(subset=["variety_code"]).copy()
    varieties = varieties.dropna(subset=["variety_code"])
    varieties = varieties.sort_values("variety_code")

    # Add grape_category
    category_map = {4: "raisin", 5: "table", 6: "wine", 7: "wine"}
    varieties["grape_category"] = varieties["grape_type_code"].map(category_map).fillna("unknown")

    return varieties.reset_index(drop=True)


def build_dim_crop_year(all_data: pd.DataFrame) -> pd.DataFrame:
    """Build crop year dimension table."""
    if all_data.empty:
        return pd.DataFrame(columns=["crop_year", "report_type", "source_url", "ingested_at"])

    years = sorted(all_data["crop_year"].dropna().unique())
    rows = []
    for year in years:
        rows.append({
            "crop_year": int(year),
            "report_type": "Final",
            "source_url": f"https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Crush/Final/{int(year)}/",
            "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(rows)


def build_all_dimensions(years: list[int]) -> None:
    """Build all dimension tables from silver parquet files."""
    print("=== Silver Layer: Building dimension tables ===")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # Load all year parquets
    frames = []
    for year in sorted(years):
        path = SILVER_DIR / f"{year}_tb08.parquet"
        if path.exists():
            frames.append(pd.read_parquet(path))

    if not frames:
        print("  No silver parquet files found!")
        return

    all_data = pd.concat(frames, ignore_index=True)
    print(f"  Combined {len(frames)} years, {len(all_data):,} total rows")

    # Build each dimension
    dim_district = build_dim_district()
    dim_district.to_parquet(SILVER_DIR / "dim_district.parquet", index=False)
    print(f"  dim_district: {len(dim_district)} rows")

    dim_grape_type = build_dim_grape_type(all_data)
    dim_grape_type.to_parquet(SILVER_DIR / "dim_grape_type.parquet", index=False)
    print(f"  dim_grape_type: {len(dim_grape_type)} rows")

    dim_variety = build_dim_grape_variety(all_data)
    dim_variety.to_parquet(SILVER_DIR / "dim_grape_variety.parquet", index=False)
    print(f"  dim_grape_variety: {len(dim_variety)} rows")

    dim_year = build_dim_crop_year(all_data)
    dim_year.to_parquet(SILVER_DIR / "dim_crop_year.parquet", index=False)
    print(f"  dim_crop_year: {len(dim_year)} rows")

    print("Dimension tables built successfully")


if __name__ == "__main__":
    years = list(range(2016, 2025))
    build_all_dimensions(years)
