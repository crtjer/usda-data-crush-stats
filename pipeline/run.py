"""Orchestrator: run full pipeline -- extract -> transform -> load -> validate."""
import argparse
import sys
import time


def parse_years(years_str: str) -> list[int]:
    """Parse '2016-2024' or '2020,2021,2024' into a list of ints."""
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
    parser.add_argument("--years", default="2000-2024")
    parser.add_argument("--year", type=int, help="Process single year")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--skip-extract", action="store_true")
    parser.add_argument("--skip-ttb", action="store_true")
    args = parser.parse_args()

    if args.year:
        years = [args.year]
    else:
        years = parse_years(args.years)

    print(f"Pipeline starting for years: {years[0]}-{years[-1]} ({len(years)} years)")
    t0 = time.time()

    # Step 1: Extract (Bronze)
    if not args.skip_extract:
        print("\n=== Step 1: Extract (Bronze) ===")
        from pipeline.extract.scrape_manifest import build_manifest
        from pipeline.extract.download_crush import download_crush

        build_manifest(years)
        download_crush(years, force=args.force)

        if not args.skip_ttb:
            from pipeline.extract.download_ttb import download_ttb
            download_ttb(years)

        from pipeline.extract.download_quickstats import download_quickstats
        download_quickstats(years)
    else:
        print("\n=== Step 1: Extract (skipped) ===")

    # Step 2: Transform (Silver)
    print("\n=== Step 2: Transform (Silver) ===")
    from pipeline.transform.parse_crush_tb08 import parse_all_crush
    from pipeline.transform.parse_ttb import parse_ttb
    from pipeline.transform.parse_acreage_pdf import parse_acreage
    from pipeline.transform.parse_quickstats import parse_quickstats

    parse_all_crush(years)
    if not args.skip_ttb:
        parse_ttb()
    parse_acreage()
    parse_quickstats()

    # Step 3: Load (Gold)
    print("\n=== Step 3: Load (Gold) ===")
    from pipeline.load.build_dimensions import build_dimensions
    from pipeline.load.build_facts import build_facts
    from pipeline.load.build_bridge import build_bridge

    build_dimensions(years)
    build_facts(years)
    build_bridge(years)

    # Step 4: Validate
    print("\n=== Step 4: Validate ===")
    from pipeline.load.validate import validate

    success = validate()

    elapsed = time.time() - t0
    print(f"\nPipeline completed in {elapsed:.1f}s")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
