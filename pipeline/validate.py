"""Validation: run data quality checks on gold layer outputs."""

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
GOLD_DIR = BASE_DIR / "data" / "gold"

EXPECTED_YEARS = set(range(2016, 2025))


def check(name: str, condition: bool, detail: str = "") -> bool:
    """Run a single check, print result."""
    status = "PASS" if condition else "FAIL"
    msg = f"  [{status}] {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    return condition


def validate() -> bool:
    """Run all validation checks. Returns True if all pass."""
    print("=== Validation: Running data quality checks ===\n")
    results = []

    # Load fact table
    fact_path = GOLD_DIR / "fact_crush_stats.csv"
    if not fact_path.exists():
        print("  [FAIL] fact_crush_stats.csv not found!")
        return False

    fact = pd.read_csv(fact_path)
    summary_path = GOLD_DIR / "fact_crush_summary.csv"
    summary = pd.read_csv(summary_path) if summary_path.exists() else pd.DataFrame()

    # Check 1: All expected years present
    present_years = set(fact["crop_year"].dropna().unique().astype(int))
    missing = EXPECTED_YEARS - present_years
    results.append(check(
        "All expected years present in fact table",
        len(missing) == 0,
        f"missing: {sorted(missing)}" if missing else f"found: {sorted(present_years)}"
    ))

    # Check 2: No null crop_year
    null_years = fact["crop_year"].isna().sum()
    results.append(check(
        "No null crop_year values",
        null_years == 0,
        f"{null_years} null rows" if null_years else ""
    ))

    # Check 3: No null variety_code in data rows
    null_variety = fact["variety_code"].isna().sum()
    results.append(check(
        "No null variety_code values",
        null_variety == 0,
        f"{null_variety} null rows" if null_variety else ""
    ))

    # Check 4: No null tons in data rows
    null_tons = fact["tons_crushed"].isna().sum()
    results.append(check(
        "No null tons_crushed values",
        null_tons == 0,
        f"{null_tons} null rows" if null_tons else ""
    ))

    # Check 5: Price values are positive where present
    prices = fact["wt_price_per_ton"].dropna()
    neg_prices = (prices < 0).sum()
    results.append(check(
        "Price values are non-negative",
        neg_prices == 0,
        f"{neg_prices} negative prices" if neg_prices else f"{len(prices):,} valid prices"
    ))

    # Check 6: All 17 districts present for each year
    for year in sorted(EXPECTED_YEARS):
        year_districts = set(fact[fact["crop_year"] == year]["district_id"].dropna().unique().astype(int))
        expected_districts = set(range(1, 18))
        missing_d = expected_districts - year_districts
        if missing_d:
            results.append(check(
                f"All 17 districts present for {year}",
                False,
                f"missing districts: {sorted(missing_d)}"
            ))
            break
    else:
        results.append(check(
            "All 17 districts present for each year",
            True,
            "districts 1-17 present for all years"
        ))

    # Check 7: Dimension tables exist and have expected sizes
    dim_district = GOLD_DIR / "dim_district.csv"
    if dim_district.exists():
        dd = pd.read_csv(dim_district)
        results.append(check(
            "dim_district has expected rows",
            len(dd) >= 18,
            f"{len(dd)} rows"
        ))
    else:
        results.append(check("dim_district exists", False))

    dim_variety = GOLD_DIR / "dim_grape_variety.csv"
    if dim_variety.exists():
        dv = pd.read_csv(dim_variety)
        results.append(check(
            "dim_grape_variety has 100+ varieties",
            len(dv) >= 100,
            f"{len(dv)} varieties"
        ))
    else:
        results.append(check("dim_grape_variety exists", False))

    # Check 8: Tons are non-negative
    neg_tons = (fact["tons_crushed"].dropna() < 0).sum()
    results.append(check(
        "No negative tonnage values",
        neg_tons == 0,
        f"{neg_tons} negative values" if neg_tons else ""
    ))

    # Check 9: Analysis files exist
    for analysis_file in ["analysis_top_varieties.csv", "analysis_price_trends.csv", "analysis_district_totals.csv"]:
        path = GOLD_DIR / analysis_file
        results.append(check(
            f"{analysis_file} exists",
            path.exists(),
            f"{len(pd.read_csv(path))} rows" if path.exists() else "not found"
        ))

    # Summary
    passed = sum(results)
    total = len(results)
    print(f"\n{'='*40}")
    print(f"Results: {passed}/{total} checks passed")
    if passed == total:
        print("All checks PASSED!")
    else:
        print(f"{total - passed} check(s) FAILED")
    print(f"{'='*40}")

    return passed == total


if __name__ == "__main__":
    import sys
    success = validate()
    sys.exit(0 if success else 1)
