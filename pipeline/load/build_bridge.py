"""Build bridge table linking crush data to TTB wine production."""
import pandas as pd

from pipeline.config import SILVER_DIR, FINAL_DIR


def build_bridge(years: list[int]) -> None:
    """Build crush-to-wine bridge table.

    Joins wine grape crush totals (grape_type_code in [6, 7, 8]) by year
    to TTB California wine production data.
    """
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    # Load crush silver data and aggregate wine grape tons by year
    crush_frames = []
    for year in years:
        path = SILVER_DIR / f"{year}_tb08.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            crush_frames.append(df)

    if not crush_frames:
        print("No crush data found for bridge table.")
        _write_empty_bridge()
        return

    crush = pd.concat(crush_frames, ignore_index=True)

    # Filter to wine grapes (type codes 6, 7, 8) and data rows
    wine_mask = crush["grape_type_code"].isin([6, 7, 8]) if "grape_type_code" in crush.columns else pd.Series(False, index=crush.index)

    # Use row_type_code == 3 (summary) or sum data rows
    tons_col = "tons" if "tons" in crush.columns else "tons_crushed" if "tons_crushed" in crush.columns else None

    if tons_col is None or not wine_mask.any():
        print("No wine grape tonnage data found for bridge table.")
        _write_empty_bridge()
        return

    # Filter to data rows (row_type_code == 2) for wine grapes
    if "row_type_code" in crush.columns:
        data_mask = crush["row_type_code"] == 2
        wine_data = crush[wine_mask & data_mask].copy()
    else:
        wine_data = crush[wine_mask].copy()

    # Aggregate wine grape tons by crop year
    wine_tons = (
        wine_data.groupby("crop_year")[tons_col]
        .sum()
        .reset_index()
        .rename(columns={tons_col: "ca_wine_tons_crushed"})
    )

    # Load TTB wine data
    ttb_path = SILVER_DIR / "ttb_wine.parquet"
    if ttb_path.exists():
        ttb = pd.read_parquet(ttb_path)
        # Filter to California totals
        if "state" in ttb.columns:
            ca_ttb = ttb[ttb["state"].str.contains("California", case=False, na=False)]
        else:
            ca_ttb = ttb

        if not ca_ttb.empty and "gallons_produced" in ca_ttb.columns:
            year_col = "year" if "year" in ca_ttb.columns else "crop_year"
            if year_col in ca_ttb.columns:
                ttb_agg = (
                    ca_ttb.groupby(year_col)["gallons_produced"]
                    .sum()
                    .reset_index()
                    .rename(columns={year_col: "crop_year", "gallons_produced": "ca_gallons_wine_produced"})
                )
            else:
                ttb_agg = pd.DataFrame(columns=["crop_year", "ca_gallons_wine_produced"])
        else:
            ttb_agg = pd.DataFrame(columns=["crop_year", "ca_gallons_wine_produced"])
    else:
        ttb_agg = pd.DataFrame(columns=["crop_year", "ca_gallons_wine_produced"])

    # Left join crush totals to TTB data
    bridge = wine_tons.merge(ttb_agg, on="crop_year", how="left")

    # Calculate ratio where both values are present
    bridge["tons_per_gallon_ratio"] = None
    mask = bridge["ca_gallons_wine_produced"].notna() & (bridge["ca_gallons_wine_produced"] > 0)
    if mask.any():
        bridge.loc[mask, "tons_per_gallon_ratio"] = (
            bridge.loc[mask, "ca_wine_tons_crushed"] / bridge.loc[mask, "ca_gallons_wine_produced"]
        )

    # Add source columns
    bridge["source_crush"] = "USDA NASS Grape Crush Report"
    bridge["source_ttb"] = None
    bridge.loc[bridge["ca_gallons_wine_produced"].notna(), "source_ttb"] = "TTB Wine Statistics"

    # Select and order columns
    bridge = bridge[
        [
            "crop_year",
            "ca_wine_tons_crushed",
            "ca_gallons_wine_produced",
            "tons_per_gallon_ratio",
            "source_crush",
            "source_ttb",
        ]
    ].sort_values("crop_year").reset_index(drop=True)

    bridge.to_csv(FINAL_DIR / "bridge_crush_to_wine.csv", index=False)
    print(f"bridge_crush_to_wine.csv: {len(bridge)} rows")


def _write_empty_bridge() -> None:
    """Write an empty bridge CSV with correct schema."""
    df = pd.DataFrame(
        columns=[
            "crop_year",
            "ca_wine_tons_crushed",
            "ca_gallons_wine_produced",
            "tons_per_gallon_ratio",
            "source_crush",
            "source_ttb",
        ]
    )
    df.to_csv(FINAL_DIR / "bridge_crush_to_wine.csv", index=False)
    print("bridge_crush_to_wine.csv: 0 rows (empty)")
