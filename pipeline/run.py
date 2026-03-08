"""Orchestrator: run full pipeline — download → parse → dimensions → gold."""

import argparse
import sys
import time
from pathlib import Path

from pipeline.download import download_all
from pipeline.parse import parse_all
from pipeline.dimensions import build_all_dimensions
from pipeline.gold import build_gold
from pipeline.validate import validate


def parse_years(years_str: str) -> list[int]:
    """Parse year specification: '2016-2024' or '2020,2021,2024'."""
    years = []
    for part in years_str.split(","):
        part = part.strip()
        if "-" in part:
            start, end = part.split("-", 1)
            years.extend(range(int(start), int(end) + 1))
        else:
            years.append(int(part))
    return sorted(set(years))


def main():
    parser = argparse.ArgumentParser(description="USDA Grape Crush Data Pipeline")
    parser.add_argument("--years", default="2000-2024",
                        help="Years to process: '2016-2024' or '2020,2021,2024'")
    parser.add_argument("--force", action="store_true",
                        help="Force re-download of all files")
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip download step, use existing bronze files")
    args = parser.parse_args()

    years = parse_years(args.years)
    print(f"USDA Grape Crush Pipeline — Processing years: {years}\n")
    start = time.time()

    # Step 1: Download
    if not args.skip_download:
        download_results = download_all(years, force=args.force)
    else:
        print("=== Skipping download (using existing bronze files) ===")
    print()

    # Step 2: Parse
    total_rows, years_parsed = parse_all(years)
    print()

    # Step 3: Dimensions
    build_all_dimensions(years)
    print()

    # Step 4: Gold
    gold_results = build_gold(years)
    print()

    # Summary
    elapsed = time.time() - start
    print(f"{'='*50}")
    print(f"Pipeline complete in {elapsed:.1f}s")
    print(f"  Years processed: {years_parsed}")
    print(f"  Total rows ingested: {total_rows:,}")
    print(f"  Gold files written: {len(gold_results)}")
    for fname, count in gold_results.items():
        print(f"    {fname}: {count:,} rows")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
