"""Pipeline configuration: paths, URLs, constants."""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
SILVER_DIR = BASE_DIR / "data" / "silver"
FINAL_DIR = BASE_DIR / "data" / "final"

CRUSH_DIR = RAW_DIR / "crush_reports"
ACREAGE_DIR = RAW_DIR / "acreage_reports"
TTB_DIR = RAW_DIR / "ttb"
QUICKSTATS_DIR = RAW_DIR / "quickstats"

NASS_BASE = "https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Crush"
NASS_FINAL = f"{NASS_BASE}/Final"
NASS_ERRATA = f"{NASS_BASE}/Errata"

SESSION_HEADERS = {"User-Agent": "USDA-Data-Pipeline/1.0 (research; crtjer@gmail.com)"}

# USDA grape type codes
GRAPE_TYPE_MAP = {
    4: ("Raisin Grapes", "raisin"),
    5: ("Table Grapes", "table"),
    6: ("Wine Grapes (Red)", "wine"),
    7: ("Wine Grapes (White)", "wine"),
    8: ("Wine Grapes (Red/Black)", "wine"),
}
