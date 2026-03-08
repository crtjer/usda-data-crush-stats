"""Gold layer: build final analytical CSVs from silver parquets."""

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"


def _load_all_silver(years: list[int]) -> pd.DataFrame:
    """Load and combine all silver year parquets."""
    frames = []
    for year in sorted(years):
        path = SILVER_DIR / f"{year}_tb08.parquet"
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_fact_crush_stats(all_data: pd.DataFrame) -> pd.DataFrame:
    """Build fact table with all data rows (row_type_code=2)."""
    df = all_data[all_data["row_type_code"] == 2].copy()
    df = df[["crop_year", "district", "grape_type_code", "grape_type_name",
             "variety_code", "variety_name", "wt_price", "brix_code",
             "brix_value", "tons", "is_state_total"]].copy()
    df = df.rename(columns={"district": "district_id", "wt_price": "wt_price_per_ton",
                            "tons": "tons_crushed"})
    df = df.reset_index(drop=True)
    df.index.name = "id"
    return df


def build_fact_crush_summary(all_data: pd.DataFrame) -> pd.DataFrame:
    """Build summary table with summary rows (row_type_code=3)."""
    df = all_data[all_data["row_type_code"] == 3].copy()
    df = df[["crop_year", "district", "grape_type_code", "grape_type_name",
             "variety_code", "variety_name", "wt_price", "tons",
             "is_state_total"]].copy()
    df = df.rename(columns={"district": "district_id", "wt_price": "avg_price_per_ton",
                            "tons": "total_tons"})
    df = df.reset_index(drop=True)
    df.index.name = "id"
    return df


def build_analysis_top_varieties(fact_stats: pd.DataFrame) -> pd.DataFrame:
    """Top 20 wine grape varieties by total tons across all years."""
    wine = fact_stats[fact_stats["grape_type_code"].isin([6, 7])].copy()
    top = (wine.groupby(["variety_code", "variety_name"])
           .agg(total_tons=("tons_crushed", "sum"),
                avg_price=("wt_price_per_ton", "mean"),
                years_present=("crop_year", "nunique"))
           .reset_index()
           .sort_values("total_tons", ascending=False)
           .head(20))
    return top.reset_index(drop=True)


def build_analysis_price_trends(fact_stats: pd.DataFrame) -> pd.DataFrame:
    """Average price per ton per year per grape type (wine white, wine red)."""
    wine = fact_stats[fact_stats["grape_type_code"].isin([6, 7])].copy()
    wine = wine.dropna(subset=["wt_price_per_ton"])
    trends = (wine.groupby(["crop_year", "grape_type_code", "grape_type_name"])
              .agg(avg_price_per_ton=("wt_price_per_ton", "mean"),
                   total_tons=("tons_crushed", "sum"),
                   num_records=("tons_crushed", "count"))
              .reset_index()
              .sort_values(["crop_year", "grape_type_code"]))
    return trends.reset_index(drop=True)


def build_analysis_district_totals(fact_stats: pd.DataFrame) -> pd.DataFrame:
    """Total tons by district by year."""
    totals = (fact_stats.groupby(["crop_year", "district_id"])
              .agg(total_tons=("tons_crushed", "sum"),
                   avg_price=("wt_price_per_ton", "mean"),
                   num_varieties=("variety_code", "nunique"))
              .reset_index()
              .sort_values(["crop_year", "district_id"]))
    return totals.reset_index(drop=True)


def build_gold(years: list[int]) -> dict:
    """Build all gold layer outputs. Returns dict of {filename: row_count}."""
    print("=== Gold Layer: Building analytical CSVs ===")
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    all_data = _load_all_silver(years)
    if all_data.empty:
        print("  No silver data found!")
        return {}

    results = {}

    # Fact tables
    fact_stats = build_fact_crush_stats(all_data)
    fact_stats.to_csv(GOLD_DIR / "fact_crush_stats.csv", index=True)
    results["fact_crush_stats.csv"] = len(fact_stats)
    print(f"  fact_crush_stats.csv: {len(fact_stats):,} rows")

    fact_summary = build_fact_crush_summary(all_data)
    fact_summary.to_csv(GOLD_DIR / "fact_crush_summary.csv", index=True)
    results["fact_crush_summary.csv"] = len(fact_summary)
    print(f"  fact_crush_summary.csv: {len(fact_summary):,} rows")

    # Dimension tables (copy from silver parquet to gold CSV)
    for dim_name in ["dim_grape_variety", "dim_district", "dim_grape_type", "dim_crop_year"]:
        parquet_path = SILVER_DIR / f"{dim_name}.parquet"
        if parquet_path.exists():
            dim_df = pd.read_parquet(parquet_path)
            dim_df.to_csv(GOLD_DIR / f"{dim_name}.csv", index=False)
            results[f"{dim_name}.csv"] = len(dim_df)
            print(f"  {dim_name}.csv: {len(dim_df)} rows")

    # Analysis tables
    top_varieties = build_analysis_top_varieties(fact_stats)
    top_varieties.to_csv(GOLD_DIR / "analysis_top_varieties.csv", index=False)
    results["analysis_top_varieties.csv"] = len(top_varieties)
    print(f"  analysis_top_varieties.csv: {len(top_varieties)} rows")

    price_trends = build_analysis_price_trends(fact_stats)
    price_trends.to_csv(GOLD_DIR / "analysis_price_trends.csv", index=False)
    results["analysis_price_trends.csv"] = len(price_trends)
    print(f"  analysis_price_trends.csv: {len(price_trends)} rows")

    district_totals = build_analysis_district_totals(fact_stats)
    district_totals.to_csv(GOLD_DIR / "analysis_district_totals.csv", index=False)
    results["analysis_district_totals.csv"] = len(district_totals)
    print(f"  analysis_district_totals.csv: {len(district_totals)} rows")

    print(f"\nGold layer complete: {len(results)} files written")
    return results


if __name__ == "__main__":
    years = list(range(2000, 2025))
    build_gold(years)
