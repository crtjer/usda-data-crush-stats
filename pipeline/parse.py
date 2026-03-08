"""Silver layer: parse bronze CSVs/XLSX into typed pandas DataFrames."""

import re
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze" / "grape_crush"
SILVER_DIR = BASE_DIR / "data" / "silver"

# Grape type name → code mapping for XLS parsing
# Actual USDA codes: 4=Raisin, 5=Table, 6=Wine Red, 7=Wine White
GRAPE_TYPE_LOOKUP = {
    "raisin grapes": (4, "Raisin Grapes"),
    "table grapes": (5, "Table Grapes"),
    "wine grapes (white)": (7, "Wine Grapes (White)"),
    "wine grapes (red/black)": (6, "Wine Grapes (Red)"),
    "wine grapes (red and black)": (6, "Wine Grapes (Red)"),
    "wine grapes (red)": (6, "Wine Grapes (Red)"),
}


def _find_tb08_files(year: int) -> list[Path]:
    """Find TB08 CSV or XLSX/XLS files for a given year."""
    year_dir = BRONZE_DIR / str(year)
    if not year_dir.exists():
        return []

    results = []

    # Look for CSV files with tb08 in name
    for f in year_dir.rglob("*.csv"):
        if "tb08" in f.name.lower() or "tb_08" in f.name.lower():
            results.append(f)

    # If no CSV, look for XLSX/XLS with tb08 in name (exclude supplements)
    if not results:
        for ext in ("*.xlsx", "*.xls"):
            for f in year_dir.rglob(ext):
                lower = f.name.lower()
                if "supplement" in lower:
                    continue
                if ext == "*.xls" and f.name.endswith(".xlsx"):
                    continue
                # Match tb08, tb_08, table08, and multi-part files like tb081, tb082
                if re.search(r'tb0?8\d?', lower) or "tb_08" in lower or "table08" in lower:
                    results.append(f)

    # Broader search for "08" in name
    if not results:
        for ext in ("*.xlsx", "*.xls"):
            for f in year_dir.rglob(ext):
                lower = f.name.lower()
                if "supplement" in lower:
                    continue
                if ext == "*.xls" and f.name.endswith(".xlsx"):
                    continue
                if "08" in lower:
                    results.append(f)

    return results


def _parse_brix_code(brix_code) -> float | None:
    """Parse brix_code to float. "024500" → 24.5, "000100"/100 → None."""
    if pd.isna(brix_code):
        return None
    brix_str = str(brix_code).strip().split(".")[0]  # remove decimal if present
    if not brix_str or brix_str in ("000100", "100", "nan"):
        return None
    try:
        val = float(brix_str)
        # If it's already a small number (< 100), it might be the raw code
        if val <= 0:
            return None
        if val < 100:
            return None  # codes like 100 = no brix
        if val > 100 and val < 50000:
            return val / 1000.0  # e.g. 24500 → 24.5
        return None
    except (ValueError, TypeError):
        return None


def _parse_xls_tb08(filepath: Path, year: int) -> pd.DataFrame | None:
    """Parse hierarchical XLS/XLSX TB08 report format.

    These files have district headers, grape type headers, and variety data rows
    in a human-readable layout, not a flat table.
    """
    engine = "openpyxl" if filepath.suffix.lower() == ".xlsx" else "xlrd"
    try:
        xl = pd.ExcelFile(filepath, engine=engine)
    except Exception:
        xl = pd.ExcelFile(filepath, engine="openpyxl")

    # Find the right sheet
    sheet_name = None
    for s in xl.sheet_names:
        sl = s.lower()
        if "tb08" in sl or "tb_08" in sl or "gcbtb" in sl:
            sheet_name = s
            break
    if not sheet_name:
        for s in xl.sheet_names:
            if "08" in s:
                sheet_name = s
                break
    if not sheet_name:
        # Use first sheet (common for older files where sheet = filename)
        sheet_name = xl.sheet_names[0]

    raw = xl.parse(sheet_name, header=None)

    # State tracking
    current_district = None
    current_grape_type_code = None
    current_grape_type_name = None
    rows = []

    # Find the data columns based on the file structure
    # Typically: col0=name, col1=price, col2(maybe empty), then brix, tons
    ncols = len(raw.columns)

    for idx in range(len(raw)):
        row = raw.iloc[idx]
        cell0 = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""

        # Skip empty rows and title rows
        if not cell0 or cell0.lower().startswith("table 8"):
            continue
        if cell0.lower().startswith("district, type"):
            continue

        # Check for DISTRICT header
        district_match = re.match(r"DISTRICT\s+(\d+)", cell0, re.IGNORECASE)
        if district_match:
            current_district = int(district_match.group(1))
            continue

        # Check for STATE TOTAL header
        if "STATE" in cell0.upper() and "TOTAL" in cell0.upper():
            current_district = 18
            continue

        # Check for grape type header
        cell0_lower = cell0.lower().strip()
        matched_type = None
        for key, (code, name) in GRAPE_TYPE_LOOKUP.items():
            if cell0_lower.startswith(key) or key in cell0_lower:
                matched_type = (code, name)
                break
        if matched_type:
            current_grape_type_code, current_grape_type_name = matched_type
            continue

        # Skip sub-header rows
        if cell0.lower() in ("dollars", "per ton", "code", "code 1", "code a/", ""):
            continue
        if cell0.lower().startswith("base price"):
            continue
        if cell0.lower().startswith("brix"):
            continue

        # Skip if we haven't established context yet
        if current_district is None or current_grape_type_code is None:
            continue

        # Determine if this is a summary row or data row
        is_summary = "wtd." in cell0.lower() or "avg." in cell0.lower() or "total" in cell0.lower()

        # Extract variety name (clean asterisks and trailing numbers)
        variety_name = cell0.strip()
        # Extract variety code if present (trailing number after variety name)
        variety_code = None
        # Pattern: "Chardonnay * 2" or "Albarino 2" — trailing digit is NOT the variety code
        # In XLS format, variety_code isn't provided directly, we derive it later
        variety_name = re.sub(r'\s*\*?\s*\d*$', '', variety_name).strip()
        variety_name = variety_name.rstrip("*").strip()

        if not variety_name:
            continue

        # Extract numeric values from remaining columns
        # The layout varies by year but generally:
        # For 5-col files: 0=name, 1=price, 2=empty/dollars, 3=brix, 4=tons
        # For 4-col files: 0=name, 1=price, 2=brix, 3=tons
        if ncols >= 5:
            price_val = row.iloc[1]
            brix_val = row.iloc[3]
            tons_val = row.iloc[4]
        else:
            price_val = row.iloc[1]
            brix_val = row.iloc[2]
            tons_val = row.iloc[3]

        # Convert to numeric
        price = pd.to_numeric(price_val, errors="coerce")
        brix_code_raw = str(brix_val).strip() if pd.notna(brix_val) else None
        tons = pd.to_numeric(tons_val, errors="coerce")

        # Skip rows with no tons data (likely a sub-header or blank)
        if pd.isna(tons):
            continue

        row_type_code = 3 if is_summary else 2
        row_type_name = "summary_row" if is_summary else "data_row"

        rows.append({
            "district": current_district,
            "grape_type_code": current_grape_type_code,
            "grape_type_name": current_grape_type_name,
            "variety_code": None,  # XLS doesn't have codes; will be assigned later
            "variety_name": variety_name,
            "wt_price": price,
            "brix_code": brix_code_raw,
            "tons": tons,
            "row_type_code": row_type_code,
            "row_type_name": row_type_name,
        })

    if not rows:
        return None

    df = pd.DataFrame(rows)

    # Assign variety codes: create a consistent mapping from variety_name
    # Use a hash-based approach for unique IDs
    unique_varieties = df["variety_name"].unique()
    name_to_code = {}
    for name in sorted(unique_varieties):
        if "wtd." in name.lower() or "avg." in name.lower() or "total" in name.lower():
            # Summary rows get code 0
            name_to_code[name] = 0
        else:
            # Generate a stable code from the name
            name_to_code[name] = abs(hash(name.lower())) % 90000 + 10000
    df["variety_code"] = df["variety_name"].map(name_to_code)

    return df


def _parse_csv_tb08(filepath: Path) -> pd.DataFrame | None:
    """Parse flat CSV TB08 file."""
    for encoding in ["utf-8", "latin-1", "cp1252"]:
        try:
            df = pd.read_csv(filepath, encoding=encoding, low_memory=False)
            if len(df.columns) >= 8:
                return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    return None


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to match expected schema."""
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    rename = {}
    for col in df.columns:
        if "district" in col and "district" not in rename.values():
            rename[col] = "district"
        elif "grape_type_code" in col or col == "grapetypecode":
            rename[col] = "grape_type_code"
        elif "grape_type_name" in col or col == "grapetypename":
            rename[col] = "grape_type_name"
        elif "variety_code" in col or col == "varietycode":
            rename[col] = "variety_code"
        elif "variety_name" in col or col == "varietyname":
            rename[col] = "variety_name"
        elif col in ("wt_price", "wtprice", "weighted_price", "price"):
            rename[col] = "wt_price"
        elif "brix_code" in col or col == "brixcode":
            rename[col] = "brix_code"
        elif col == "tons" or col == "tons_crushed":
            rename[col] = "tons"
        elif "row_type_code" in col or col == "rowtypecode":
            rename[col] = "row_type_code"
        elif "row_type_name" in col or col == "rowtypename":
            rename[col] = "row_type_name"

    if rename:
        df = df.rename(columns=rename)
    return df


def parse_year(year: int) -> pd.DataFrame | None:
    """Parse TB08 data for a single year."""
    files = _find_tb08_files(year)
    if not files:
        print(f"  [{year}] No TB08 files found")
        return None

    # Parse all matching files and combine (handles multi-part files like tb081/tb082)
    frames = []
    parsed_names = []

    for f in sorted(files):
        if f.suffix.lower() == ".csv":
            part_df = _parse_csv_tb08(f)
            if part_df is not None:
                part_df = _normalize_columns(part_df)
                frames.append(part_df)
                parsed_names.append(f.name)
                break  # CSV files contain all data
        elif f.suffix.lower() in (".xls", ".xlsx"):
            part_df = _parse_xls_tb08(f, year)
            if part_df is not None:
                frames.append(part_df)
                parsed_names.append(f.name)

    if not frames:
        print(f"  [{year}] No readable/parseable files found")
        return None

    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    names_str = " + ".join(parsed_names)
    print(f"  [{year}] Read {names_str}: {len(df)} rows, {len(df.columns)} cols")

    # Check we have required columns
    required = ["district", "variety_code", "tons"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"  [{year}] Missing columns: {missing}. Available: {list(df.columns)}")
        return None

    # Cast types
    df["district"] = pd.to_numeric(df["district"], errors="coerce").astype("Int64")
    df["grape_type_code"] = pd.to_numeric(df.get("grape_type_code"), errors="coerce").astype("Int64")
    df["variety_code"] = pd.to_numeric(df["variety_code"], errors="coerce").astype("Int64")
    df["wt_price"] = pd.to_numeric(df.get("wt_price"), errors="coerce")
    df["tons"] = pd.to_numeric(df["tons"], errors="coerce")
    df["row_type_code"] = pd.to_numeric(df.get("row_type_code"), errors="coerce").astype("Int64")

    # Parse brix_code to brix_value
    if "brix_code" in df.columns:
        df["brix_code"] = df["brix_code"].astype(str).str.strip()
        df["brix_value"] = df["brix_code"].apply(_parse_brix_code)
    else:
        df["brix_code"] = None
        df["brix_value"] = None

    # Add crop year
    df["crop_year"] = year

    # Flag state totals (district 0 or 18)
    df["is_state_total"] = df["district"].isin([0, 18])

    # Ensure all expected columns exist
    for col in ["grape_type_name", "variety_name", "row_type_name"]:
        if col not in df.columns:
            if col == "row_type_name":
                df[col] = df["row_type_code"].map({2: "data_row", 3: "summary_row"})
            else:
                df[col] = None

    # Clean variety_name
    df["variety_name"] = df["variety_name"].astype(str).str.strip().str.rstrip("*").str.strip()
    df["grape_type_name"] = df["grape_type_name"].astype(str).str.strip()

    # Drop rows where key fields are null
    df = df.dropna(subset=["district", "variety_code"], how="all")

    return df


def parse_all(years: list[int]) -> tuple[int, int]:
    """Parse all years and save silver parquets. Returns (total_rows, years_parsed)."""
    print("=== Silver Layer: Parsing raw data ===")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    all_frames = []
    years_parsed = 0

    for year in sorted(years):
        df = parse_year(year)
        if df is not None and len(df) > 0:
            out_path = SILVER_DIR / f"{year}_tb08.parquet"
            df.to_parquet(out_path, index=False)
            print(f"  [{year}] Saved {out_path.name} ({len(df)} rows)")
            all_frames.append(df)
            years_parsed += 1

    total_rows = sum(len(df) for df in all_frames)
    print(f"\nParsing complete: {years_parsed} years, {total_rows:,} total rows")
    return total_rows, years_parsed


if __name__ == "__main__":
    years = list(range(2000, 2025))
    parse_all(years)
