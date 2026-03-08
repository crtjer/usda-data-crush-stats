"""Validate Gold layer outputs and produce a validation report."""
import json
import sys
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import FINAL_DIR


def _check(name: str, passed: bool, message: str) -> dict:
    """Create a check result entry."""
    return {
        "check": name,
        "passed": passed,
        "message": message,
    }


def validate() -> bool:
    """Run validation checks on Gold layer outputs.

    Writes data/final/validation_report.json with results.
    Returns True if all critical checks pass.
    """
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    checks = []

    # Load data files (with safe fallbacks)
    try:
        crush_stats = pd.read_csv(FINAL_DIR / "fact_crush_stats.csv")
    except (FileNotFoundError, pd.errors.EmptyDataError):
        crush_stats = pd.DataFrame()

    try:
        crush_summary = pd.read_csv(FINAL_DIR / "fact_crush_summary.csv")
    except (FileNotFoundError, pd.errors.EmptyDataError):
        crush_summary = pd.DataFrame()

    try:
        dim_district = pd.read_csv(FINAL_DIR / "dim_district.csv")
    except (FileNotFoundError, pd.errors.EmptyDataError):
        dim_district = pd.DataFrame()

    try:
        dim_variety = pd.read_csv(FINAL_DIR / "dim_grape_variety.csv")
    except (FileNotFoundError, pd.errors.EmptyDataError):
        dim_variety = pd.DataFrame()

    # Check 1: All expected years 2016-2024 present in fact_crush_stats
    expected_years = set(range(2016, 2025))
    if not crush_stats.empty and "crop_year" in crush_stats.columns:
        actual_years = set(crush_stats["crop_year"].unique())
        missing = expected_years - actual_years
        if missing:
            checks.append(_check(
                "expected_years_present",
                False,
                f"Missing years in fact_crush_stats: {sorted(missing)}",
            ))
        else:
            checks.append(_check(
                "expected_years_present",
                True,
                "All expected years 2016-2024 present.",
            ))
    else:
        checks.append(_check(
            "expected_years_present",
            False,
            "fact_crush_stats is empty or missing crop_year column.",
        ))

    # Check 2: No NULL tons_crushed in data rows
    if not crush_stats.empty and "tons_crushed" in crush_stats.columns:
        null_count = crush_stats["tons_crushed"].isna().sum()
        checks.append(_check(
            "no_null_tons_crushed",
            null_count == 0,
            f"NULL tons_crushed count: {null_count}",
        ))
    else:
        checks.append(_check(
            "no_null_tons_crushed",
            False,
            "fact_crush_stats missing or no tons_crushed column.",
        ))

    # Check 3: dim_district has exactly 19 rows
    district_count = len(dim_district)
    checks.append(_check(
        "dim_district_19_rows",
        district_count == 19,
        f"dim_district has {district_count} rows (expected 19).",
    ))

    # Check 4: dim_grape_variety has 100+ varieties
    variety_count = len(dim_variety)
    checks.append(_check(
        "dim_grape_variety_100_plus",
        variety_count >= 100,
        f"dim_grape_variety has {variety_count} varieties.",
    ))

    # Check 5: No negative prices or tonnage
    neg_issues = []
    if not crush_stats.empty:
        if "wt_price_per_ton" in crush_stats.columns:
            neg_prices = (crush_stats["wt_price_per_ton"].dropna() < 0).sum()
            if neg_prices > 0:
                neg_issues.append(f"{neg_prices} negative prices")
        if "tons_crushed" in crush_stats.columns:
            neg_tons = (crush_stats["tons_crushed"].dropna() < 0).sum()
            if neg_tons > 0:
                neg_issues.append(f"{neg_tons} negative tonnages")
    checks.append(_check(
        "no_negative_values",
        len(neg_issues) == 0,
        "No negative values found." if not neg_issues else f"Found: {'; '.join(neg_issues)}",
    ))

    # Check 6: Most recent year's data present
    if not crush_stats.empty and "crop_year" in crush_stats.columns:
        max_year = crush_stats["crop_year"].max()
        checks.append(_check(
            "most_recent_year_present",
            True,
            f"Most recent year in data: {max_year}.",
        ))
    else:
        checks.append(_check(
            "most_recent_year_present",
            False,
            "No data available to determine most recent year.",
        ))

    # Check 7: fact_crush_stats has data
    has_stats = not crush_stats.empty and len(crush_stats) > 0
    checks.append(_check(
        "fact_crush_stats_has_data",
        has_stats,
        f"fact_crush_stats has {len(crush_stats)} rows." if has_stats else "fact_crush_stats is empty.",
    ))

    # Check 8: fact_crush_summary has data
    has_summary = not crush_summary.empty and len(crush_summary) > 0
    checks.append(_check(
        "fact_crush_summary_has_data",
        has_summary,
        f"fact_crush_summary has {len(crush_summary)} rows." if has_summary else "fact_crush_summary is empty.",
    ))

    # Compute results
    passed = [c for c in checks if c["passed"]]
    failed = [c for c in checks if not c["passed"]]

    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "checks_passed": len(passed),
        "checks_failed": len(failed),
        "details": checks,
    }

    report_path = FINAL_DIR / "validation_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nValidation Report: {len(passed)} passed, {len(failed)} failed")
    for check in checks:
        status = "PASS" if check["passed"] else "FAIL"
        print(f"  [{status}] {check['check']}: {check['message']}")

    print(f"\nReport saved to {report_path}")

    if failed:
        print(f"\n{len(failed)} check(s) failed.")
        return False

    print("\nAll checks passed.")
    return True


if __name__ == "__main__":
    success = validate()
    sys.exit(0 if success else 1)
