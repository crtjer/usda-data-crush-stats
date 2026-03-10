"""Build fact CSV tables for the Gold layer."""
import hashlib

import pandas as pd

from pipeline.config import SILVER_DIR, FINAL_DIR, TTB_DIR


def _generate_id(*parts) -> str:
    """Generate a deterministic hash ID from the given parts."""
    key = "|".join(str(p) for p in parts)
    return hashlib.md5(key.encode()).hexdigest()[:16]


def _load_crush_silver(years: list[int]) -> pd.DataFrame:
    """Load and combine crush silver parquet files."""
    frames = []
    for year in years:
        path = SILVER_DIR / f"{year}_tb08.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _load_dim_crop_year() -> pd.DataFrame:
    """Load dim_crop_year with source_url for provenance joins."""
    dim_path = FINAL_DIR / "dim_crop_year.csv"
    if not dim_path.exists():
        return pd.DataFrame(columns=["crop_year", "source_url"])

    try:
        df = pd.read_csv(dim_path, usecols=["crop_year", "source_url"])
    except ValueError:
        # Fallback if columns are missing or schema changed
        return pd.DataFrame(columns=["crop_year", "source_url"])

    return df


def _build_fact_crush_stats(crush_df: pd.DataFrame) -> pd.DataFrame:
    """Build fact_crush_stats from data rows (row_type_code == 2)."""
    if crush_df.empty or "row_type_code" not in crush_df.columns:
        return pd.DataFrame(
            columns=[
                "id",
                "crop_year",
                "district_id",
                "variety_code",
                "grape_type_code",
                "brix_code",
                "brix_value",
                "wt_price_per_ton",
                "tons_crushed",
                "report_type",
                "source_url",
            ]
        )

    df = crush_df[crush_df["row_type_code"] == 2].copy()

    # Rename columns to match Gold schema
    rename_map = {}
    if "district" in df.columns:
        rename_map["district"] = "district_id"
    if "wt_price" in df.columns:
        rename_map["wt_price"] = "wt_price_per_ton"
    if "tons" in df.columns:
        rename_map["tons"] = "tons_crushed"
    if rename_map:
        df = df.rename(columns=rename_map)

    # Generate deterministic IDs
    id_cols = ["crop_year", "district_id", "variety_code", "grape_type_code", "brix_code"]
    existing_id_cols = [c for c in id_cols if c in df.columns]
    df["id"] = df.apply(
        lambda row: _generate_id(*[row.get(c, "") for c in existing_id_cols]),
        axis=1,
    )

    # Add report_type
    if "report_type" not in df.columns:
        df["report_type"] = "Final"

    # Join in source_url from dim_crop_year for provenance
    if "crop_year" in df.columns:
        dim_year = _load_dim_crop_year()
        if not dim_year.empty:
            df = df.merge(dim_year, on="crop_year", how="left")

    output_cols = [
        "id",
        "crop_year",
        "district_id",
        "variety_code",
        "grape_type_code",
        "brix_code",
        "brix_value",
        "wt_price_per_ton",
        "tons_crushed",
        "report_type",
        "source_url",
    ]
    # Keep only columns that exist
    available = [c for c in output_cols if c in df.columns]
    return df[available].reset_index(drop=True)


def _build_fact_crush_summary(crush_df: pd.DataFrame) -> pd.DataFrame:
    """Build fact_crush_summary from summary rows (row_type_code == 3)."""
    if crush_df.empty or "row_type_code" not in crush_df.columns:
        return pd.DataFrame(
            columns=[
                "id",
                "crop_year",
                "district_id",
                "variety_code",
                "grape_type_code",
                "total_tons",
                "avg_price_per_ton",
                "source_url",
            ]
        )

    df = crush_df[crush_df["row_type_code"] == 3].copy()

    # Rename columns
    rename_map = {}
    if "district" in df.columns:
        rename_map["district"] = "district_id"
    if "tons" in df.columns:
        rename_map["tons"] = "total_tons"
    if "wt_price" in df.columns:
        rename_map["wt_price"] = "avg_price_per_ton"
    if rename_map:
        df = df.rename(columns=rename_map)

    # Generate IDs
    id_cols = ["crop_year", "district_id", "variety_code", "grape_type_code"]
    existing_id_cols = [c for c in id_cols if c in df.columns]
    df["id"] = df.apply(
        lambda row: _generate_id("summary", *[row.get(c, "") for c in existing_id_cols]),
        axis=1,
    )

    # Join in source_url from dim_crop_year for provenance
    if "crop_year" in df.columns:
        dim_year = _load_dim_crop_year()
        if not dim_year.empty:
            df = df.merge(dim_year, on="crop_year", how="left")

    output_cols = [
        "id",
        "crop_year",
        "district_id",
        "variety_code",
        "grape_type_code",
        "total_tons",
        "avg_price_per_ton",
        "source_url",
    ]
    available = [c for c in output_cols if c in df.columns]
    return df[available].reset_index(drop=True)


def _build_fact_acreage() -> pd.DataFrame:
    """Build fact_acreage from acreage silver data."""
    acreage_path = SILVER_DIR / "acreage.parquet"
    if not acreage_path.exists():
        print("No acreage silver data found; writing empty fact_acreage.csv")
        return pd.DataFrame(
            columns=[
                "id",
                "crop_year",
                "variety_code",
                "bearing_acres",
                "non_bearing_acres",
                "total_acres",
            ]
        )

    df = pd.read_parquet(acreage_path)

    id_cols = ["crop_year", "variety_code"]
    existing_id_cols = [c for c in id_cols if c in df.columns]
    df["id"] = df.apply(
        lambda row: _generate_id("acreage", *[row.get(c, "") for c in existing_id_cols]),
        axis=1,
    )

    output_cols = [
        "id",
        "crop_year",
        "variety_code",
        "bearing_acres",
        "non_bearing_acres",
        "total_acres",
    ]
    available = [c for c in output_cols if c in df.columns]
    return df[available].reset_index(drop=True)


def _build_fact_ttb_wine() -> pd.DataFrame:
    """Build fact_ttb_wine from TTB silver data."""
    ttb_path = SILVER_DIR / "ttb_wine.parquet"
    if not ttb_path.exists():
        print("No TTB wine silver data found; writing empty fact_ttb_wine.csv")
        return pd.DataFrame(
            columns=[
                "id",
                "year",
                "state",
                "wine_type",
                "gallons_produced",
                "gallons_removed",
                "gallons_on_hand",
            ]
        )

    df = pd.read_parquet(ttb_path)

    id_cols = ["year", "state", "wine_type"]
    existing_id_cols = [c for c in id_cols if c in df.columns]
    df["id"] = df.apply(
        lambda row: _generate_id("ttb", *[row.get(c, "") for c in existing_id_cols]),
        axis=1,
    )

    output_cols = [
        "id",
        "year",
        "state",
        "wine_type",
        "gallons_produced",
        "gallons_removed",
        "gallons_on_hand",
    ]
    available = [c for c in output_cols if c in df.columns]
    return df[available].reset_index(drop=True)


def build_facts(years: list[int]) -> dict:
    """Build all fact CSV tables and write to data/final/.

    Returns dict of {filename: row_count}.
    """
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    results = {}

    # Load crush silver data
    crush_df = _load_crush_silver(years)

    # fact_crush_stats
    stats = _build_fact_crush_stats(crush_df)
    stats.to_csv(FINAL_DIR / "fact_crush_stats.csv", index=False)
    results["fact_crush_stats.csv"] = len(stats)
    print(f"fact_crush_stats.csv: {len(stats)} rows")

    # fact_crush_summary
    summary = _build_fact_crush_summary(crush_df)
    summary.to_csv(FINAL_DIR / "fact_crush_summary.csv", index=False)
    results["fact_crush_summary.csv"] = len(summary)
    print(f"fact_crush_summary.csv: {len(summary)} rows")

    # fact_acreage
    acreage = _build_fact_acreage()
    acreage.to_csv(FINAL_DIR / "fact_acreage.csv", index=False)
    results["fact_acreage.csv"] = len(acreage)
    print(f"fact_acreage.csv: {len(acreage)} rows")

    # fact_ttb_wine
    ttb = _build_fact_ttb_wine()
    ttb.to_csv(FINAL_DIR / "fact_ttb_wine.csv", index=False)
    results["fact_ttb_wine.csv"] = len(ttb)
    print(f"fact_ttb_wine.csv: {len(ttb)} rows")

    print("Facts built successfully.")
    return results
